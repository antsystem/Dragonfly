/*
 * Copyright The Dragonfly Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package seed

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	url2 "net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	api_types "github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/config"
	dfdaemonDownloader "github.com/dragonflyoss/Dragonfly/dfdaemon/downloader"
	dfgetcfg "github.com/dragonflyoss/Dragonfly/dfget/config"
	coreAPI "github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"github.com/dragonflyoss/Dragonfly/dfget/corev2/basic"
	"github.com/dragonflyoss/Dragonfly/dfget/corev2/pattern/seed/api"
	uploader2 "github.com/dragonflyoss/Dragonfly/dfget/corev2/uploader"
	"github.com/dragonflyoss/Dragonfly/dfget/local/seed"
	"github.com/dragonflyoss/Dragonfly/dfget/types"
	"github.com/dragonflyoss/Dragonfly/pkg/algorithm"
	"github.com/dragonflyoss/Dragonfly/pkg/constants"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/dragonflyoss/Dragonfly/pkg/protocol"
	"github.com/dragonflyoss/Dragonfly/pkg/queue"

	downloader2 "github.com/dragonflyoss/Dragonfly/dfget/corev2/downloader"
	shutdown2 "github.com/dragonflyoss/Dragonfly/dfget/corev2/shutdown"
	"github.com/go-openapi/strfmt"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
)

type rangeRequestExtra struct {
	filters map[string]map[string]bool
}

type rangeRequest struct {
	oriURL string
	url    string
	off    int64
	size   int64
	header map[string]string
	extra  *rangeRequestExtra
}

func (rr rangeRequest) OriURL() string {
	return rr.oriURL
}

func (rr rangeRequest) URL() string {
	return rr.url
}

func (rr rangeRequest) Offset() int64 {
	return rr.off
}

func (rr rangeRequest) Size() int64 {
	return rr.size
}

func (rr rangeRequest) Header() map[string]string {
	return rr.header
}

func (rr rangeRequest) Extra() interface{} {
	return rr.extra
}

type seedURL struct {
	rawURL string
	url    string
}

func newSeedURL(url string) *seedURL {
	return &seedURL{
		rawURL: url,
		url:    httputils.URLStripQuery(url),
	}
}

const (
	// 512KB
	defaultBlockOrder = 19

	maxTry = 3

	// unit: hour.
	defaultSeedExpireDuration = 1

	// 1 GB
	defaultDownloadCacheWaterMark = 1 * int64(1024*1024*1024)

	defaultDownloadTimeout = time.Second * 2
)

var (
	localManager *Manager
	once         sync.Once
)

func init() {
	dfdaemonDownloader.Register("seed", func(patternConfig config.PatternConfig, commonCfg config.DFGetCommonConfig, c config.Properties) dfdaemonDownloader.Stream {
		return NewManager(patternConfig, commonCfg, c)
	})
}

// Manager provides an implementation of downloader.Stream.
//
type Manager struct {
	//client        protocol.Client
	downloaderAPI api.DownloadAPI
	supernodeAPI  coreAPI.SupernodeAPI

	seedManager seed.Manager
	sm          *supernodeManager
	evQueue     queue.Queue

	bm *blackListManger

	downloadCache *downloadCacheManager

	cfg *Config

	ctx    context.Context
	cancel func()
}

func NewManager(patternConfig config.PatternConfig, commonCfg config.DFGetCommonConfig, c config.Properties) *Manager {
	once.Do(func() {
		m := newManager(patternConfig, commonCfg, c)
		localManager = m
	})

	return localManager
}

func (m *Manager) graceShutdown() {
	m.seedManager.Stop()
	_, sds, _ := m.seedManager.List()
	for _, sd := range sds {
		m.reportSeedPrepareDelete(sd.GetURL(), sd.GetTaskID())
	}
	logrus.Info("seed manager shutdown")
}

func newManager(pCfg config.PatternConfig, commonCfg config.DFGetCommonConfig, config config.Properties) *Manager {
	cfg := &Config{}
	data, err := json.Marshal(pCfg.Opts)
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(data, cfg)
	if err != nil {
		panic(err)
	}

	cfg.DFGetCommonConfig = commonCfg
	useTLS := (config.CertPem != "" && config.KeyPem != "")

	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		cfg:           cfg,
		supernodeAPI:  coreAPI.NewSupernodeAPI(),
		downloaderAPI: api.NewDownloadAPI(useTLS),
		bm:            newBlackListManger(0),
		ctx:           ctx,
		cancel:        cancel,
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

	if cfg.ExpireDuration <= 0 {
		cfg.ExpireDuration = defaultSeedExpireDuration
	}

	if cfg.DownloadCacheWaterMark <= 0 {
		cfg.DownloadCacheWaterMark = defaultDownloadCacheWaterMark
	}

	cfg.expireDuration = time.Duration(cfg.ExpireDuration) * time.Hour

	config.SuperNodes = algorithm.DedupStringArr(config.SuperNodes)
	m.sm = newSupernodeManager(ctx, cfg, config.SuperNodes, m.supernodeAPI, intervalOpt{})
	seedDownFactory := newDownloaderFactory(m.sm, &api_types.PeerInfo{
		IP:   strfmt.IPv4(cfg.IP),
		Port: int32(cfg.Port),
		ID:   cfg.Cid,
	}, api.NewDownloadAPI(useTLS))

	m.seedManager = seed.NewSeedManager(seed.NewSeedManagerOpt{
		StoreDir:           filepath.Join(cfg.WorkHome, "localSeed"),
		ConcurrentLimit:    cfg.ConcurrentLimit,
		TotalLimit:         cfg.TotalLimit,
		DownloadBlockOrder: uint32(cfg.DefaultBlockOrder),
		OpenMemoryCache:    !cfg.DisableOpenMemoryCache,
		DownloadRate:       int64(cfg.DownRate),
		UploadRate:         int64(cfg.UploadRate),
		HighLevel:          uint(cfg.HighLevel),
		LowLevel:           uint(cfg.LowLevel),
		Factory:            seedDownFactory,
	})
	if m.cfg.UseDownloadCache {
		m.downloadCache = NewDownloadCacheManager(ctx, m.cfg.DownloadCacheWaterMark, int64(1<<uint(m.cfg.DefaultBlockOrder)))
	}

	uploader2.RegisterUploader("seed", newUploader(m.seedManager))
	m.restoreLocalSeed(ctx, true, true)
	go m.handleSuperNodeEventLoop(ctx)
	shutdown2.RegisterShutdown(shutdown)

	return m
}

func shutdown() {
	if localManager != nil {
		localManager.graceShutdown()
	}
}

func localIp() string {
	if localManager != nil {
		return localManager.cfg.IP
	}
	return ""
}

func getLocalManager() *Manager {
	if localManager != nil {
		return localManager
	}
	return nil
}

func (m *Manager) getDownloadURL(oriURL, name string) string {
	switch name {
	case "oss", "http", "https":
		u, err := url2.Parse(oriURL)
		if err != nil {
			return ""
		}
		return u.Query().Get("download_url")
	default:
		return oriURL
	}
}

// DownloadStreamContext implementation of downloader.Stream.
func (m *Manager) DownloadStreamContext(ctx context.Context, oriURL string, header map[string][]string, name string) (int64, io.ReadCloser, error) {
	reqRange, err := m.getRangeFromHeader(header)
	if err != nil {
		return -1, nil, err
	}

	if reqRange.EndIndex < 0 {
		reqRange.EndIndex = math.MaxInt64
	}
	downloadURL := m.getDownloadURL(oriURL, name)
	if downloadURL == "" {
		return 404, nil, fmt.Errorf("Invailid URL")
	}
	u := newSeedURL(downloadURL)
	m.sm.AddRequest(u.url)

	logrus.Debugf("start to download stream in seed pattern, url: %s, header: %v, range: [%d, %d]", u.url,
		header, reqRange.StartIndex, reqRange.EndIndex)

	downloadFn := func(ctx context.Context, oriHeader map[string][]string, off, size int64) (int64, io.ReadCloser, error) {
		m.sm.AddRequest(u.url)

		logrus.Debugf("download url: %s, range: [%d, %d]", u.url, off, off+size-1)
		hdr := HeaderToMap(oriHeader)

		dwInfos := []*basic.SchedulePeerInfo{}
		rr := &rangeRequest{
			oriURL: u.rawURL,
			url:    u.url,
			off:    off,
			size:   size,
			header: hdr,
		}

		for i := 0; i < maxTry; i++ {
			// try to get the peer by internal schedule
			dwInfos = m.sm.Schedule(ctx, rr)
			if len(dwInfos) > 0 {
				break
			}
			m.sm.AddRequest(u.url)
			// try to apply to be the seed node
			m.tryToApplyForSeedNode(m.ctx, u, oriHeader)
		}
		return m.runStream(ctx, rr, dwInfos)
	}
	if m.cfg.UseDownloadCache {
		dataLen, rc, err := m.downloadCache.StreamContext(u.url, CopyHeader(header), reqRange.StartIndex, reqRange.EndIndex-reqRange.StartIndex+1, downloadFn)
		if err == nil {
			return dataLen, rc, nil
		}
		logrus.Warnf("download from cache err! url: %s, header: %v, err: %v", u.url, header, err)
	}
	return downloadFn(ctx, header, reqRange.StartIndex, reqRange.EndIndex-reqRange.StartIndex+1)
}

func (m *Manager) runStream(ctx context.Context, rr basic.RangeRequest, peers []*basic.SchedulePeerInfo) (int64, io.ReadCloser, error) {
	var (
		rc      io.ReadCloser
		err     error
		dataLen int64
	)

	for _, peer := range peers {
		// check on black list
		if !m.bm.check(rr.URL(), peer.ID) {
			continue
		}
		dataLen, rc, err = m.tryToDownload(ctx, peer, rr)
		if err == nil {
			break
		}
		// download failed, add into black list
		m.bm.add(rr.URL(), peer.ID)
	}

	if rc == nil {
		logrus.Errorf("failed to select a peer to download %v, download from source", err)
		if err != nil && strings.Contains(err.Error(), "out of range") {
			// we never retry if meets out of range error
			return 500, nil, err
		}
		if dataLen, rc, err = m.tryToDownload(ctx, nil, rr); err != nil {
			return dataLen, nil, err
		}
	}

	if m.cfg.UseDownloadCache {
		// in cache mode, we already divide request into pieces
		return dataLen, rc, nil
	}

	var (
		pr, pw = io.Pipe()
	)

	// todo: divide request data into pieces in consideration of peer disconnect.
	go func(sourceRc io.ReadCloser) {
		cw := NewClientWriter()
		notify, err := cw.Run(ctx, pw)
		if err != nil {
			pw.CloseWithError(err)
			sourceRc.Close()
			return
		}

		defer func() {
			<-notify.Done()
			sourceRc.Close()
		}()

		data, _ := NewSeedData(sourceRc, rr.Size(), true)
		cw.PutData(data)
		cw.PutData(protocol.NewEoFDistributionData())
	}(rc)

	return dataLen, pr, nil
}

func (m *Manager) tryToDownload(ctx context.Context, peer *basic.SchedulePeerInfo, rr basic.RangeRequest) (int64, io.ReadCloser, error) {
	var down downloader2.Downloader
	if peer == nil {
		down = NewSourceDownloader(rr.OriURL(), rr.Header(), defaultDownloadTimeout)
	} else if m.cfg.Cid == peer.ID {
		sd, err := m.seedManager.Get(peer.Path)
		if err != nil {
			return 404, nil, err
		}
		m.seedManager.RefreshExpireTime(peer.Path, 0)
		down = NewLocalDownloader(sd)
	} else {
		down = NewDownloader(peer, 0, m.downloaderAPI)
	}
	dataLen, rc, err := down.Download(ctx, rr.Offset(), rr.Size())
	if err != nil {
		logrus.Errorf("try to download err: %v", err)
		errCounter.WithLabelValues("download failed").Inc()
		return 500, nil, err
	}
	peerPair := "source"
	if peer != nil {
		peerPair = fmt.Sprintf("%s:%d", peer.IP, peer.Port)
	}

	start := time.Now()
	wrapRc := httputils.NewWithFuncReadCloser(rc, func() {
		recordDownloadCostTimer(dataLen, peerPair, time.Now().Sub(start))
		recordDownloadFlowCounter(dataLen, peerPair)
	})

	return dataLen, wrapRc, nil
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
		if ds[0].EndIndex == math.MaxInt64-1 {
			ds[0].EndIndex = ds[0].StartIndex - 1
		}

		return ds[0], nil
	}

	return &httputils.RangeStruct{
		StartIndex: 0,
		EndIndex:   -1,
	}, nil
}

func (m *Manager) fetchP2PNetwork(ctx context.Context, url string, force, nowait bool) {
	waitCh := make(chan struct{})
	m.sm.ActiveFetchP2PNetwork(activeFetchSt{
		url:    url,
		waitCh: waitCh,
		force:  force,
	})
	if nowait {
		return
	}
	select {
	case <-ctx.Done():
		return
	case <-waitCh:
	}
}

func (m *Manager) tryToApplyForSeedNode(ctx context.Context, u *seedURL, header map[string][]string) {
	path := uuid.New()
	cHeader := CopyHeader(header)
	hr := http.Header(cHeader)
	hr.Del(dfgetcfg.StrRange)

	asSeed, taskID, fullLength := m.applyForSeedNode(u, hr, path)
	if !asSeed {
		m.fetchP2PNetwork(ctx, u.url, false, false)
		return
	}

	m.fetchP2PNetwork(ctx, u.url, true, true)
	err := m.registerLocalSeed(u, fullLength, cHeader, path, taskID, defaultBlockOrder)
	if err != nil {
		errCounter.WithLabelValues("register seed failed").Inc()
		m.reportSeedPrepareDelete(u.url, taskID)
		return
	}
	go m.tryToPrefetchSeedFile(ctx, path, taskID, defaultBlockOrder)
}

func (m *Manager) applyForSeedNode(u *seedURL, header map[string][]string, path string) (asSeed bool, seedTaskID string, length int64) {
	req := &types.RegisterRequest{
		RawURL:     u.rawURL,
		TaskURL:    u.url,
		Cid:        m.cfg.Cid,
		Headers:    FlattenHeader(header),
		Dfdaemon:   m.cfg.Dfdaemon,
		IP:         m.cfg.IP,
		Port:       m.cfg.Port,
		Version:    m.cfg.Version,
		Identifier: m.cfg.Identifier,
		RootCAs:    m.cfg.RootCAs,
		HostName:   m.cfg.HostName,
		AsSeed:     true,
		Path:       path,
	}

	node := m.sm.GetSupernode(u.url)
	if node == "" {
		errCounter.WithLabelValues("no supernode").Inc()
		logrus.Errorf("failed to found supernode %s in register map", node)
		return false, "", 0
	}

	resp, err := m.supernodeAPI.ApplyForSeedNode(node, req)
	if err != nil {
		errCounter.WithLabelValues("apply for seed failed").Inc()
		logrus.Errorf("failed to apply for seed node: %v", err)
		return false, "", 0
	}

	logrus.Debugf("ApplyForSeedNode resp body: %v", resp)

	if resp.Code != constants.Success {
		return false, "", 0
	}

	return resp.Data.AsSeed, resp.Data.SeedTaskID, resp.Data.FileLength
}

// syncLocalSeed will sync local seed to all scheduler.
func (m *Manager) syncLocalSeed(path string, taskID string, sd seed.Seed) {
	m.sm.AddLocalSeed(path, taskID, sd)
}

func (m *Manager) registerLocalSeed(u *seedURL, fullLength int64, header map[string][]string, path string, taskID string, blockOrder uint32) error {
	info := seed.BaseInfo{
		RawURL:        u.rawURL,
		URL:           u.url,
		Header:        header,
		BlockOrder:    blockOrder,
		ExpireTimeDur: m.cfg.expireDuration,
		TaskID:        taskID,
		FullLength:    fullLength,
	}
	sd, err := m.seedManager.Register(path, info)
	if err == errortypes.ErrTaskIDDuplicate {
		return nil
	}

	if err != nil {
		logrus.Errorf("failed to register seed, info: %v, err:%v", info, err)
		return err
	}

	m.syncLocalSeed(path, taskID, sd)
	return nil
}

func (m *Manager) cleanSeed(path string) {
	sd, err := m.seedManager.Get(path)
	if err != nil {
		return
	}
	m.reportSeedPrepareDelete(sd.GetURL(), sd.GetTaskID())
	m.removeLocalSeedFromScheduler(sd.GetURL())
	m.seedManager.UnRegister(path)
}

// tryToPrefetchSeedFile will try to prefetch the seed file
func (m *Manager) tryToPrefetchSeedFile(ctx context.Context, path string, taskID string, blockOrder uint32) {
	finishCh, err := m.seedManager.Prefetch(path, m.computePerDownloadSize(blockOrder))
	if err != nil {
		logrus.Errorf("failed to prefetch: %v", err)
		return
	}

	<-finishCh

	result, err := m.seedManager.GetPrefetchResult(path)
	if err != nil {
		logrus.Errorf("failed to get prefetch result: %v", err)
		return
	}

	if !result.Success {
		logrus.Warnf("path: %s, taskID: %s, prefetch result : %v, do clean seed", path, taskID, result)
		// clean seed
		m.cleanSeed(path)
		return
	}

	go m.monitorExpiredSeed(ctx, path)
}

// monitor the expired event of seed
func (m *Manager) monitorExpiredSeed(ctx context.Context, path string) {
	sd, err := m.seedManager.Get(path)
	if err != nil {
		logrus.Errorf("failed to get seed file %s: %v", path, err)
		return
	}
	// after prefetch total file, report resource again
	m.reportLocalSeedToSuperNode(path, sd)

	// if a seed is prepared to be expired, the expired chan will be notified.
	expiredCh, err := m.seedManager.NotifyPrepareExpired(path)
	if err != nil {
		logrus.Errorf("failed to get expired chan of seed, url:%s, key: %s: %v", sd.GetURL(), sd.GetTaskID(), err)
		return
	}

	reportSeedTimer := time.NewTicker(15 * time.Second)
	defer reportSeedTimer.Stop()

	expired := false
	for !expired {
		select {
		case <-ctx.Done():
			return
		case <-expiredCh:
			logrus.Infof("seed url: %s, key: %s, has been expired, try to clear resource of it", sd.GetURL(), sd.GetTaskID())
			expired = true
		case <-reportSeedTimer.C:
			m.reportLocalSeedToSuperNode(path, sd)
		}
	}

	timer := time.NewTimer(60 * time.Second)
	defer timer.Stop()

	logrus.Infof("seed %s, url %s will be deleted after %d seconds", path, sd.GetURL(), 60)

	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		logrus.Infof("start to delete seed %s(%s)", path, sd.GetURL())
	}

	// report the seed prepare to delete to super node
	m.reportSeedPrepareDelete(sd.GetURL(), sd.GetTaskID())
	// confirm all peer update the p2p network
	time.Sleep(20 * time.Second)
	// try to clear resource and report to super node
	m.removeLocalSeedFromScheduler(sd.GetURL())

	// unregister the seed file
	m.seedManager.UnRegister(path)
}

func (m *Manager) removeLocalSeedFromScheduler(url string) {
	m.sm.RemoveLocalSeed(url)
}

func (m *Manager) computePerDownloadSize(blockOrder uint32) int64 {
	return (1 << blockOrder) * int64(m.cfg.PerDownloadBlocks)
}

func (m *Manager) reportSeedPrepareDelete(url string, taskID string) bool {
	node := m.sm.GetSupernode(url)
	if node == "" {
		return true
	}

	deleted := m.reportSeedPrepareDeleteToSuperNodes(taskID, node)
	if !deleted {
		return false
	}

	return true
}

func (m *Manager) reportSeedPrepareDeleteToSuperNodes(taskID string, node string) bool {
	resp, err := m.supernodeAPI.ReportResourceDeleted(node, taskID, m.cfg.Cid)
	if err != nil {
		return true
	}

	return resp.Code == constants.CodeGetPeerDown
}

func (m *Manager) reportLocalSeedToSuperNode(path string, sd seed.Seed) {
	targetSuperNode := m.sm.GetSupernode(sd.GetURL())
	// TODO: if all supernodes are down
	if targetSuperNode == "" {
		return
	}
	req := &types.RegisterRequest{
		RawURL:     sd.GetURL(),
		TaskURL:    sd.GetURL(),
		TaskID:     sd.GetTaskID(),
		Cid:        m.cfg.Cid,
		Headers:    FlattenHeader(sd.GetHeaders()),
		Dfdaemon:   m.cfg.Dfdaemon,
		IP:         m.cfg.IP,
		Port:       m.cfg.Port,
		Version:    m.cfg.Version,
		Identifier: m.cfg.Identifier,
		RootCAs:    m.cfg.RootCAs,
		HostName:   m.cfg.HostName,
		AsSeed:     true,
		Path:       path,
		FileLength: sd.GetFullSize(),
	}

	resp, err := m.supernodeAPI.ReportResource(targetSuperNode, req)
	if err != nil || resp.Code != constants.Success {
		logrus.Errorf("failed to report resource to supernode, resp: %v, err: %v", resp, err)
	}
}

// restoreLocalSeed will report local seed to supernode
func (m *Manager) restoreLocalSeed(ctx context.Context, syncLocal bool, monitor bool) {
	keys, sds, err := m.seedManager.List()
	if err != nil {
		logrus.Errorf("failed to list local seeds : %v", err)
		return
	}

	for i := 0; i < len(keys); i++ {
		m.reportLocalSeedToSuperNode(keys[i], sds[i])
		if syncLocal {
			m.syncLocalSeed(keys[i], sds[i].GetTaskID(), sds[i])
		}
		if monitor {
			go m.monitorExpiredSeed(ctx, keys[i])
		}
	}
}

func (m *Manager) reportLocalSeedsToSuperNode() {
	keys, sds, err := m.seedManager.List()
	if err != nil {
		logrus.Errorf("failed to list local seeds : %v", err)
		return
	}

	for i := 0; i < len(keys); i++ {
		m.reportLocalSeedToSuperNode(keys[i], sds[i])
	}
}

// handleSuperNodeEventLoop handles the events of supernode, the events includes connected/disconnected/reconnected.
func (m *Manager) handleSuperNodeEventLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			break
		}

		ev, ok := m.sm.GetSupernodeEvent(time.Second * 2)
		if !ok {
			continue
		}

		m.handleSuperNodeEvent(ev)
	}
}

func (m *Manager) handleSuperNodeEvent(ev *supernodeEvent) {
	h, ok := getSEHandler(ev.evType, m)
	if ok {
		h.Handle(ev)
	}
}
