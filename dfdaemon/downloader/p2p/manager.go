package p2p

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/scheduler"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/seed"
	dfgetcfg "github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/go-openapi/strfmt"
	"github.com/pborman/uuid"
	"path/filepath"
	"sync"
	"time"

	api_types "github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"github.com/dragonflyoss/Dragonfly/dfget/types"
	"github.com/dragonflyoss/Dragonfly/pkg/constants"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/sirupsen/logrus"
	"io"
	"math"
	"net/http"
)

var(
	localManager  *Manager
	once sync.Once
)

const(
	defaultUploadRate = 100 * 1024 * 1024

	defaultDownloadRate = 100 * 1024 * 1024

	// 512KB
	defaultBlockOrder = 19
)

// Manager control the
type Manager struct {
	cfg          *Config
	sdOpt        *seed.NewSeedManagerOpt
	superNodes	 []string
	sm 			 *scheduler.Manager
	seedManager  seed.SeedManager
	supernodeAPI api.SupernodeAPI
	downloadAPI  api.DownloadAPI
	uploaderAPI  api.UploaderAPI

	rm		 *requestManager

	ctx      context.Context
	cancel   func()

	syncP2PNetworkCh	chan activeFetchSt
	syncTimeLock		sync.Mutex
	syncTime			time.Time

	// recentFetchUrls is the urls which as the parameters to fetch the p2p network recently
	recentFetchUrls     []string
}

type activeFetchSt struct {
	url   	string
	waitCh  chan struct{}
}

func GetManager() *Manager {
	return localManager
}

func NewManager(cfg *Config, superNodes []string) *Manager {
	once.Do(func() {
		m := newManager(cfg, superNodes)
		localManager = m
	})

	return localManager
}

func newManager(cfg *Config, superNodes []string) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		cfg: cfg,
		superNodes: superNodes,
		sm: scheduler.NewScheduler(&api_types.PeerInfo{
			ID: cfg.Cid,
			IP: strfmt.IPv4(cfg.IP),
			Port: int32(cfg.Port),
			HostName: strfmt.Hostname(cfg.HostName),
		}),
		supernodeAPI: api.NewSupernodeAPI(),
		uploaderAPI: api.NewUploaderAPI(time.Duration(0)),
		downloadAPI: api.NewDownloadAPI(),
		syncP2PNetworkCh: make(chan activeFetchSt, 10),
		rm: newRequestManager(),
		recentFetchUrls: []string{},
		ctx: ctx,
		cancel: cancel,
	}

	if cfg.HighLevel <= 0 {
		cfg.HighLevel = 90
	}

	if cfg.LowLevel <= 0 {
		cfg.LowLevel = 80
	}

	if cfg.DefaultBlockOrder <= 0 {
		cfg.DefaultBlockOrder = defaultBlockOrder
	}

	if cfg.PerDownloadBlocks <= 0 {
		cfg.PerDownloadBlocks = 4
	}

	if cfg.PerDownloadBlocks >= 1000 {
		cfg.PerDownloadBlocks = 1000
	}

	if cfg.TotalLimit <= 0 {
		cfg.TotalLimit = 50
	}

	if cfg.ConcurrentLimit <= 0 {
		cfg.ConcurrentLimit = 4
	}

	m.sdOpt = &seed.NewSeedManagerOpt{
		StoreDir: filepath.Join(cfg.MetaDir, "seedStore"),
		TotalLimit: cfg.TotalLimit,
		ConcurrentLimit: cfg.ConcurrentLimit,
		DownloadBlockOrder: uint32(cfg.DefaultBlockOrder),
		OpenMemoryCache: true,
		// todo: set rate of download and upload by config.
		DownloadRate: int64(cfg.DownRate),
		UploadRate: int64(cfg.DownRate),
		HighLevel:  uint(cfg.HighLevel),
		LowLevel:   uint(cfg.LowLevel),
	}

	seedManager := seed.NewSeedManager(*m.sdOpt)

	m.seedManager = seedManager

	go m.fetchP2PNetworkInfoLoop(ctx)
	go m.heartBeatLoop(ctx)

	return m
}

func (m *Manager) DownloadStreamContext(ctx context.Context, url string, header map[string][]string, name string) (io.Reader, error) {
	reqRange, err := m.getRangeFromHeader(header)
	if err != nil {
		return nil, err
	}

	m.rm.addRequest(url)

	logrus.Debugf("start to download stream in seed pattern, url: %s, header: %v, range: [%d, %d]", url,
		header, reqRange.StartIndex, reqRange.EndIndex)

	// 改为for循环
schedule:
	// try to get the peer by internal schedule
	result := m.sm.Scheduler(ctx, url)
	if len(result) == 0 {
		// try to apply to be the seed node
		m.tryToApplyForSeedNode(m.ctx, url, header)
		goto schedule
	}

	dwInfos := []*downloadNodeInfo{}
	for _, r := range result {
		dwInfos = append(dwInfos, &downloadNodeInfo{
			ip: r.PeerInfo.IP.String(),
			port: int(r.PeerInfo.Port),
			path: r.Path,
			peerID: r.DstCid,
		})
	}

	dw := &SeedDownloader{
		selectNodes: dwInfos,
		reqRange: reqRange,
		url: url,
		header: header,
		downloadAPI: m.downloadAPI,
	}

	return dw.RunStream(ctx)
}

func (m *Manager) tryToApplyForSeedNode(ctx context.Context, url string, header map[string][]string)  {
	path := uuid.New()
	cHeader := CopyHeader(header)
	hr := http.Header(cHeader)
	hr.Del(dfgetcfg.StrRange)

	asSeed, taskID := m.applyForSeedNode(url, cHeader, path)
	if ! asSeed {
		waitCh := make(chan struct{})
		m.syncP2PNetworkCh <- activeFetchSt{url: url, waitCh: waitCh}
		<- waitCh
		return
	}

	m.registerLocalSeed(url, cHeader, path, taskID, defaultBlockOrder)
	go m.tryToPrefetchSeedFile(ctx, path, taskID, defaultBlockOrder)
}

// sync p2p network to local scheduler.
func (m *Manager) syncP2PNetworkInfo(urls []string) {
	if len(urls) == 0 {
		logrus.Debugf("no urls to syncP2PNetworkInfo")
		return
	}

	resp, err := m.fetchP2PNetwork(urls)
	if err != nil {
		logrus.Error(err)
		return
	}

	// update nodes info to internal scheduler
	m.sm.SyncSchedulerInfo(resp.Nodes)

	m.syncTimeLock.Lock()
	defer m.syncTimeLock.Unlock()
	m.syncTime = time.Now()
	m.recentFetchUrls = urls
}

func (m *Manager) fetchP2PNetworkInfoLoop(ctx context.Context) {
	var(
		lastTime time.Time
	)
	defaultInterval := 5 * time.Second
	ticker := time.NewTicker(defaultInterval)
	defer ticker.Stop()

	for{
		select {
		case <- ctx.Done():
			return
		case <- ticker.C:
			m.syncTimeLock.Lock()
			lastTime = m.syncTime
			m.syncTimeLock.Unlock()

			if lastTime.Add(defaultInterval).After(time.Now()) {
				continue
			}

			m.syncP2PNetworkInfo(m.rm.getRecentRequest(0))
		case active := <- m.syncP2PNetworkCh:
			if m.isRecentFetch(active.url) {
				if active.waitCh != nil {
					close(active.waitCh)
				}
				// the url is fetch recently, directly ignore it
				continue
			}
			m.syncP2PNetworkInfo(m.rm.getRecentRequest(0))
			if active.waitCh != nil {
				close(active.waitCh)
			}
		}
	}
}

func (m *Manager) isRecentFetch(url string) bool {
	m.syncTimeLock.Lock()
	defer m.syncTimeLock.Unlock()

	for _, u := range m.recentFetchUrls {
		if u == url {
			return true
		}
	}

	return false
}

func (m *Manager) getRangeFromHeader(header map[string][]string) (*httputils.RangeStruct, error) {
	hr := http.Header(header)
	if headerStr := hr.Get(dfgetcfg.StrRange); headerStr != "" {
		ds, err := httputils.GetRangeSE(headerStr, math.MaxInt64)
		if err != nil {
			return nil, err
		}

		// todo: support the multi range
		if len(ds) != 1 {
			return nil, fmt.Errorf("not support multi range")
		}

		// if EndIndex is max int64, set EndIndex to (StartIndex - 1),
		// so that means the end index is tail of file length.
		if ds[0].EndIndex == math.MaxInt64 - 1 {
			ds[0].EndIndex = ds[0].StartIndex - 1
		}

		return ds[0], nil
	}

	return &httputils.RangeStruct{
		StartIndex: 0,
		EndIndex: -1,
	}, nil
}

func (m *Manager) applyForSeedNode(url string, header map[string][]string, path string) (asSeed bool, seedTaskID string) {
	req := &types.RegisterRequest{
		RawURL: url,
		TaskURL: url,
		Cid: m.cfg.Cid,
		Headers: FlattenHeader(header),
		Dfdaemon: m.cfg.Dfdaemon,
		IP:  m.cfg.IP,
		Port: m.cfg.Port,
		Version: m.cfg.Version,
		Identifier: m.cfg.Identifier,
		RootCAs: m.cfg.RootCAs,
		HostName: m.cfg.HostName,
		AsSeed: true,
		Path: path,
	}

	for _, node := range m.superNodes {
		resp, err := m.supernodeAPI.ApplyForSeedNode(node, req)
		if err != nil {
			logrus.Errorf("failed to apply for seed node: %v", err)
			continue
		}

		logrus.Debugf("ApplyForSeedNode resp body: %v", resp)

		if resp.Code != constants.Success {
			continue
		}

		return resp.Data.AsSeed,  resp.Data.SeedTaskID
	}

	return false, ""
}

func (m *Manager) fetchP2PNetwork(urls []string) (resp *api_types.NetworkInfoFetchResponse, e error) {
	var(
		lastErr  error
	)

	req := &api_types.NetworkInfoFetchRequest{
		Urls: urls,
	}

	for _, node := range m.superNodes {
		resp, err := m.supernodeAPI.FetchP2PNetworkInfo(node, 0, 0, req)
		if err != nil {
			lastErr = err
			logrus.Errorf("failed to apply for seed node: %v", err)
			continue
		}

		logrus.Debugf("FetchP2PNetworkInfo resp body: %v", resp)
		return resp, nil
	}

	return nil, lastErr
}

func (m *Manager) syncLocalSeed (path string, taskID string, sd seed.Seed) {
	m.sm.AddLocalSeedInfo(&api_types.TaskFetchInfo{
		Task: &api_types.TaskInfo{
			ID: taskID,
			AsSeed: true,
			FileLength: sd.GetFullSize(),
			RawURL: sd.URL(),
			TaskURL: sd.URL(),
		},
		Pieces: []*api_types.PieceInfo{
			{
				Path: path,
			},
		},
	})
}

func (m *Manager) heartBeatLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second * 30)
	defer ticker.Stop()

	for{
		select {
		case <- ctx.Done():
			return
		case <- ticker.C:
			m.heartbeat()
		}
	}
}

func (m *Manager) heartbeat() {
	for _,node := range m.superNodes {
		resp, err := m.supernodeAPI.HeartBeat(node, &api_types.HeartBeatRequest{
			IP: m.cfg.IP,
			Port: int32(m.cfg.Port),
			CID: m.cfg.Cid,
		})

		logrus.Debugf("heart beat resp: %v", resp)

		if err != nil {
			logrus.Errorf("failed to heart beat: %v", err)
		}
	}
}

func (m *Manager) registerLocalSeed(url string, header map[string][]string, path string, taskID string, blockOrder uint32) {
	info := seed.PreFetchInfo{
		URL: url,
		Header: header,
		BlockOrder: blockOrder,
		ExpireTimeDur: time.Hour,
		TaskID: taskID,
	}
	sd, err := m.seedManager.Register(path, info)
	if err == errortypes.ErrTaskIDDuplicate{
		return
	}

	if err != nil {
		logrus.Errorf("failed to register seed, info: %v, err:%v", info, err)
		return
	}

	m.syncLocalSeed(path, taskID, sd)
}

// tryToPrefetchSeedFile will try to prefetch the seed file
func (m *Manager) tryToPrefetchSeedFile(ctx context.Context, path string, taskID string, blockOrder uint32) {
	finishCh, err := m.seedManager.Prefetch(path, m.computePerDownloadSize(blockOrder))
	if err != nil {
		logrus.Errorf("failed to prefetch: %v", err)
		return
	}

	<- finishCh

	result, err := m.seedManager.GetPrefetchResult(path)
	if err != nil {
		logrus.Errorf("failed to get prefetch result: %v", err)
		return
	}

	if ! result.Success {
		logrus.Warnf("path: %s, taskID: %s, prefetch result : %v", path, taskID, result)
		return
	}

	go m.monitorExpiredSeed(ctx, path)
	//go lm.reportSeedToSuperNode(sd)
}

// monitor the expired event of seed
func (m *Manager) monitorExpiredSeed(ctx context.Context, path string) {
	sd, err := m.seedManager.Get(path)
	if err != nil {
		logrus.Errorf("failed to get seed file %s: %v", path, err)
		return
	}

	// if a seed is prepared to be expired, the expired chan will be notified.
	expiredCh, err := m.seedManager.NotifyPrepareExpired(path)
	if err != nil {
		logrus.Errorf("failed to get expired chan of seed, url:%s, key: %s: %v", sd.URL(), sd.TaskID(), err)
		return
	}

	select {
	case <- ctx.Done():
		return
	case <- expiredCh:
		logrus.Infof("seed url: %s, key: %s, has been expired, try to clear resource of it", sd.URL(), sd.TaskID())
		break
	}

	timer := time.NewTimer(60 *time.Second)
	defer timer.Stop()

	for {
		needBreak := false
		select {
		case <-ctx.Done():
			return
		case <- timer.C:
			logrus.Infof("seed %s, url %s will be deleted after %d seconds", path, sd.URL(), 60)
			needBreak = true
			break
		default:
		}

		if needBreak {
			break
		}

		// report the seed prepare to delete to super node
		if m.reportSeedPrepareDelete(sd.TaskID()) {
			break
		}

		time.Sleep(20 * time.Second)
	}

	// try to clear resource and report to super node
	m.sm.DeleteLocalSeedInfo(sd.URL())

	// unregister the seed file
	m.seedManager.UnRegister(path)
}

func (m *Manager) computePerDownloadSize(blockOrder uint32) int64 {
	return (1 << blockOrder) * int64(m.cfg.PerDownloadBlocks)
}

func (m *Manager) reportSeedPrepareDelete(taskID string) bool {
	deleted := m.reportSeedPrepareDeleteToSuperNodes(taskID)
	if !deleted {
		return false
	}

	return true
	//time.Sleep(20 * time.Second)
	//return m.reportSeedPrepareDeleteToSuperNodes(taskID)
}

func (m *Manager) reportSeedPrepareDeleteToSuperNodes(taskID string) bool {
	for _, node := range m.superNodes {
		resp, err := m.supernodeAPI.ReportResourceDeleted(node, taskID, m.cfg.Cid)
		if err != nil {
			continue
		}

		return resp.Code == constants.CodeGetPeerDown
	}

	return false
}
