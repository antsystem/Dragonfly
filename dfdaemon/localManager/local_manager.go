package localManager

import (
	"context"
	"fmt"
	types2 "github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/config"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/downloader/p2p"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/scheduler"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/transport"
	dfgetcfg "github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"github.com/dragonflyoss/Dragonfly/dfget/core/helper"
	"github.com/dragonflyoss/Dragonfly/dfget/types"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	strfmt "github.com/go-openapi/strfmt"
	"io"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var(
	once sync.Once
	localManager *LocalManager
)

// LocalManager will handle all the request, it will
type LocalManager struct {
	sm 		*scheduler.SchedulerManager
	supernodeAPI api.SupernodeAPI
	downloadAPI  api.DownloadAPI
	uploaderAPI  api.UploaderAPI

	dfGetConfig  *dfgetcfg.Config
	cfg config.DFGetConfig

	rm		 *requestManager

	syncP2PNetworkCh	chan string

	syncTimeLock		sync.Mutex
	syncTime			time.Time

	// recentFetchUrls is the urls which as the parameters to fetch the p2p network recently
	recentFetchUrls     []string
}

func NewLocalManager(cfg config.DFGetConfig) *LocalManager {
	once.Do(func() {
		dfcfg := convertToDFGetConfig(cfg, nil)
		localPeer := &types2.PeerInfo{
			ID: dfcfg.RV.Cid,
			IP: strfmt.IPv4(dfcfg.RV.LocalIP),
			Port: int32(dfcfg.RV.PeerPort),
		}

		localManager = &LocalManager{
			sm: scheduler.NewScheduler(localPeer),
			supernodeAPI: api.NewSupernodeAPI(),
			downloadAPI: api.NewDownloadAPI(),
			uploaderAPI: api.NewUploaderAPI(30 * time.Second),
			rm: newRequestManager(),
			dfGetConfig:  dfcfg,
			cfg: cfg,
			syncTime: time.Now(),
			syncP2PNetworkCh: make(chan string, 2),
		}

		go localManager.fetchLoop(context.Background())
		go localManager.syncLocalTask(context.Background())
	})

	return localManager
}

func (lm *LocalManager) fetchLoop(ctx context.Context) {
	var(
		lastTime time.Time
	)
	defaultInterval := 30 * time.Second
	ticker := time.NewTicker(defaultInterval)
	defer ticker.Stop()

	for{
		select {
			case <- ctx.Done():
				return
			case <- ticker.C:
				lm.syncTimeLock.Lock()
				lastTime = lm.syncTime
				lm.syncTimeLock.Unlock()

				if lastTime.Add(defaultInterval).After(time.Now()) {
					continue
				}

				lm.syncP2PNetworkInfo(lm.rm.getRecentRequest(0))
			case url := <- lm.syncP2PNetworkCh:
				if lm.isRecentFetch(url) {
					// the url is fetch recently, directly ignore it
					continue
				}
				lm.syncP2PNetworkInfo(lm.rm.getRecentRequest(0))
		}
	}
}

func (lm *LocalManager) isRecentFetch(url string) bool {
	lm.syncTimeLock.Lock()
	defer lm.syncTimeLock.Unlock()

	for _, u := range lm.recentFetchUrls {
		if u == url {
			return true
		}
	}

	return false
}

func convertToDFGetConfig(cfg config.DFGetConfig, oldCfg *dfgetcfg.Config) *dfgetcfg.Config {
	sign := fmt.Sprintf("%d-%.3f",
		os.Getpid(), float64(time.Now().UnixNano())/float64(time.Second))

	newCfg:= &dfgetcfg.Config{
		Nodes:    cfg.SuperNodes,
		DFDaemon: true,
		Pattern:  dfgetcfg.PatternCDN,
		Sign: sign,
		RV: dfgetcfg.RuntimeVariable{
			LocalIP:  cfg.LocalIP,
			PeerPort: cfg.PeerPort,
			SystemDataDir: cfg.DFRepo,
			DataDir: cfg.DFRepo,
			Cid: fmt.Sprintf("%s-%s", cfg.LocalIP, sign),
		},
	}

	if oldCfg != nil {
		newCfg.RV.Cid = oldCfg.RV.Cid
		newCfg.Sign = oldCfg.Sign
	}

	return newCfg
}

//
func (lm *LocalManager) DownloadStreamContext(ctx context.Context, url string, header map[string][]string, name string) (io.Reader, error) {
	var(
		infos = []*downloadNodeInfo{}
		rd io.Reader
		localDownloader  *LocalDownloader
		nWare		transport.NumericalWare
		key			string
	)

	nWareOb := ctx.Value("numericalWare")
	ware, ok := nWareOb.(transport.NumericalWare)
	if ok {
		nWare = ware
	}

	keyOb := ctx.Value("key")
	k, ok := keyOb.(string)
	if ok {
		key = k
	}

	startTime := time.Now()

	info := &downloadNodeInfo{
		directSource: true,
		url: url,
		header: header,
	}
	infos = append(infos, info)

	defer lm.rm.addRequest(url, false)

	taskID := lm.getDigestFromHeader(url, header)
	length := lm.getLengthFromHeader(url, header)

	logrus.Infof("start to download, url: %s, header: %v, taskID: %s, length: %d", url,
		header, taskID, length)


	// try to download from peer by internal schedule
	if taskID != "" {
		// local schedule
		result, err := lm.sm.SchedulerByTaskID(ctx, taskID, lm.dfGetConfig.RV.Cid, "", 0)
		if nWare != nil {
			nWare.Add(key, transport.ScheduleName, time.Since(startTime).Nanoseconds())
			startTime = time.Now()
		}
		if err != nil {
			go lm.scheduleBySuperNode(ctx, url, header, name, taskID, length)
			goto localDownload
		}

		tmpInfos := make([]*downloadNodeInfo, len(result))
		for i, r := range result {
			tmpInfos[i] = &downloadNodeInfo{
				ip: r.PeerInfo.IP.String(),
				port: int(r.PeerInfo.Port),
				path: r.Pieces[0].Path,
				peerID: r.PeerInfo.ID,
				local: r.Local,
			}
		}

		infos = append(tmpInfos, infos...)
	}

localDownload:
	localDownloader = NewLocalDownloader()
	localDownloader.selectNodes = infos
	localDownloader.length = length
	localDownloader.taskID = taskID
	localDownloader.systemDataDir = lm.dfGetConfig.RV.SystemDataDir
	localDownloader.outPath = helper.GetServiceFile(name, lm.dfGetConfig.RV.SystemDataDir)
	localDownloader.downloadAPI = lm.downloadAPI
	localDownloader.superAPI = lm.supernodeAPI
	localDownloader.uploaderAPI = lm.uploaderAPI
	localDownloader.config = lm.dfGetConfig
	localDownloader.header = header
	localDownloader.url = url
	localDownloader.taskFileName = name
	localDownloader.postNotifyUploader = func(req *api.FinishTaskRequest) {
		localTask := &types2.TaskFetchInfo{
			Task: &types2.TaskInfo{
				ID: req.TaskID,
				FileLength: req.Other.FileLength,
				HTTPFileLength: req.Other.FileLength,
				//Headers: req.Other.Headers,
				PieceSize: int32(req.Other.FileLength),
				PieceTotal: 1,
				TaskURL: req.Other.TaskURL,
				RawURL: req.Other.RawURL,
			},
			Pieces: []*types2.PieceInfo{
					{
						Path: req.TaskFileName,
					},
			},
		}
		lm.sm.AddLocalTaskInfo(localTask)
	}

	rd, err := localDownloader.RunStream(ctx)
	logrus.Infof("return io.read: %v", rd)
	return rd, err
}

func (lm *LocalManager) scheduleBySuperNode(ctx context.Context, url string, header map[string][]string, name string, taskID string, length int64)  {
	lm.rm.addRequest(url, false)
	lm.notifyFetchP2PNetwork(url)
}

func (lm *LocalManager) notifyFetchP2PNetwork(url string) {
	lm.syncP2PNetworkCh <- url
}

func (lm *LocalManager) isDownloadDirectReturnSrc(ctx context.Context, url string) (bool, error) {
	rs, err := lm.rm.getRequestState(url)
	if err != nil {
		if err == errortypes.ErrDataNotFound {
			return false, nil
		}

		return false, err
	}

	return rs.needReturnSrc(), nil
}

// downloadFromPeer download file from peer node.
// param:
// 	taskFileName: target file name
func (lm *LocalManager) downloadFromPeer(peer *types2.PeerInfo, taskFileName string, taskInfo *types2.TaskInfo) (io.Reader, error) {
	resp, err := lm.downloadAPI.Download(peer.IP.String(), int(peer.Port), &api.DownloadRequest{Path: taskFileName,
		PieceRange: fmt.Sprintf("%d-%d", 0, taskInfo.FileLength - 1), PieceNum: 1, PieceSize: int32(taskInfo.FileLength)}, 30 * time.Second)

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("resp code is %d, not %d", resp.StatusCode, http.StatusOK)
	}

	// todo: close the body
	return resp.Body, nil
}

func (lm *LocalManager) getDigestFromHeader(url string, header map[string][]string) string {
	hr := http.Header(header)
	if digestHeaderStr := hr.Get(dfgetcfg.StrDigest); digestHeaderStr != "" {
		ds, err := p2p.GetDigestFromHeader(digestHeaderStr)
		if err != nil {
			return ""
		}

		// todo: support the merge request
		if len(ds) != 1 {
			return ""
		}

		return ds[0].Digest
	}

	return ""
}

func (lm *LocalManager) getLengthFromHeader(url string, header map[string][]string) int64 {
	hr := http.Header(header)
	if headerStr := hr.Get(dfgetcfg.StrRange); headerStr != "" {
		ds, err := httputils.GetRangeSE(headerStr, math.MaxInt64)
		if err != nil {
			return 0
		}

		// todo: support the merge request
		if len(ds) != 1 {
			return 0
		}

		return (ds[0].EndIndex - ds[0].StartIndex + 1)
	}

	return 0
}

// sync p2p network，this function should called by
func (lm *LocalManager) syncP2PNetworkInfo(urls []string) {
	if len(urls) == 0 {
		logrus.Infof("no urls to syncP2PNetworkInfo")
		return
	}
	nodes, err := lm.fetchP2PNetworkInfo(urls)
	if err != nil {
		logrus.Errorf("failed to fetchP2PNetworkInfo: %v", err)
		return
	}

	lm.sm.SyncSchedulerInfo(nodes)
	logrus.Infof("success to sync schedule info")
	lm.syncTimeLock.Lock()
	defer lm.syncTimeLock.Unlock()
	lm.syncTime = time.Now()
	lm.recentFetchUrls = urls
}

func (lm *LocalManager) fetchP2PNetworkInfo(urls []string) ([]*types2.Node, error) {
	req := &types.FetchP2PNetworkInfoRequest{
		Urls: urls,
	}

	for _, node := range lm.dfGetConfig.Nodes {
		result, err := lm.fetchP2PNetworkFromSupernode(node, req)
		if err != nil {
			continue
		}

		return result, nil
	}

	return nil, nil
}

func (lm *LocalManager) fetchP2PNetworkFromSupernode(node string, req *types.FetchP2PNetworkInfoRequest) ([]*types2.Node, error) {
	var(
		start int = 0
		limit int =100
	)

	result := []*types2.Node{}
	for {
		resp, err := lm.supernodeAPI.FetchP2PNetworkInfo(node, start, limit, req)
		if err != nil {
			return nil, err
		}

		if resp.Code != http.StatusOK {
			return nil, fmt.Errorf("failed to fetch p2p network info: %s", resp.Msg)
		}

		result = append(result, resp.Data.Nodes...)
		if len(resp.Data.Nodes) < limit {
			break
		}
	}

	return result, nil
}

// syncLocalTask fetch local tasks to add to local schedule
func (lm *LocalManager) syncLocalTask(ctx context.Context) {
	for {
		check := lm.checkUploader(ctx, 30 * time.Second)
		if !check {
			time.Sleep(time.Minute)
			continue
		}

		break
	}

	// call it firstly
	lm.fetchAndSyncLocalTask()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for{
		select {
		    case <- ctx.Done():
				return
			case <- ticker.C:
				lm.fetchAndSyncLocalTask()
		}
	}
}

func (lm *LocalManager) checkUploader(ctx context.Context, timeout time.Duration) bool {
	ticker := time.NewTicker(time.Second * 1)
	t := time.NewTimer(timeout)
	defer t.Stop()
	defer ticker.Stop()

	for{
		select {
			case <- ticker.C:
				if lm.uploaderAPI.PingServer(lm.cfg.LocalIP, lm.cfg.PeerPort) {
					return true
				}
			case <- t.C:
				return false

			case <- ctx.Done():
				return false
		}
	}

	return false
}

func (lm *LocalManager) fetchAndSyncLocalTask() {
	result, err := lm.uploaderAPI.FetchLocalTask(lm.cfg.LocalIP, lm.cfg.PeerPort)
	if err != nil {
		logrus.Errorf("failed to fetch local task: %v", err)
		return
	}

	lm.sm.SyncLocalTaskInfo(result)
}
