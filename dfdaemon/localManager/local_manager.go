package localManager

import (
	"context"
	"fmt"
	types2 "github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/config"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/downloader/p2p"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/scheduler"
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	dfgetcfg "github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/dfget/types"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
)

// LocalManager will handle all the request, it will
type LocalManager struct {
	sm 		*scheduler.SchedulerManager
	supernodeAPI api.SupernodeAPI
	downloadAPI  api.DownloadAPI

	dfGetConfig  *dfgetcfg.Config
	cfg config.DFGetConfig

	rm		 *requestManager
}

func NewLocalManager(cfg config.DFGetConfig) *LocalManager {
	lm := &LocalManager{
		sm: scheduler.NewScheduler(),
		supernodeAPI: api.NewSupernodeAPI(),
		downloadAPI: api.NewDownloadAPI(),
		rm: newRequestManager(),
		dfGetConfig: convertToDFGetConfig(cfg),
		cfg: cfg,
	}

	return lm
}

func (lm *LocalManager) fetchLoop(ctx context.Context) {
	defaultInterval := 30 * time.Second
	ticker := time.NewTicker(defaultInterval)
	defer ticker.Stop()

	for{
		select {
			case <- ctx.Done():
				return
			case <- ticker.C:
				lm.syncP2PNetworkInfo(lm.rm.getRecentRequest())
		}
	}
}

func convertToDFGetConfig(cfg config.DFGetConfig) *dfgetcfg.Config {
	return &dfgetcfg.Config{
		Nodes:    cfg.SuperNodes,
		DFDaemon: true,
		Pattern:  dfgetcfg.PatternCDN,
		Sign: fmt.Sprintf("%d-%.3f",
			os.Getpid(), float64(time.Now().UnixNano())/float64(time.Second)),
		RV: dfgetcfg.RuntimeVariable{
			LocalIP:  cfg.LocalIP,
			PeerPort: cfg.PeerPort,
		},
	}
}

//
func (lm *LocalManager) DownloadStreamContext(ctx context.Context, url string, header map[string][]string, name string) (io.Reader, error) {
	var(
		infos = []*downloadNodeInfo{}
		rd io.Reader
		localDownloader  *LocalDownloader
	)

	defer lm.rm.addRequest(url)

	taskID := lm.getDigestFromHeader(url, header)
	length := lm.getLengthFromHeader(url, header)

	// firstly, try to download direct from source url
	directDownload, err := lm.isDownloadDirectReturnSrc(ctx, url)
	if err != nil {
		logrus.Error(err)
	}

	if directDownload {
		info := &downloadNodeInfo{
			directSource: true,
			url: url,
			header: header,
		}
		infos = append(infos, info)
		goto localDownload
	}

	// download from peer by internal schedule
	if taskID != "" {
		// local schedule
		result, err := lm.sm.SchedulerByTaskID(ctx, taskID, lm.dfGetConfig.RV.Cid, "", 0)
		if err != nil {
			goto superNodeSchedule
		}

		infos = make([]*downloadNodeInfo, len(result))
		for i, r := range result {
			infos[i] = &downloadNodeInfo{
				ip: r.PeerInfo.IP.String(),
				port: int(r.PeerInfo.Port),
				path: r.Pieces[0].Path,
				peerID: r.PeerInfo.ID,
			}
		}
	}

localDownload:
	localDownloader = NewLocalDownloader()
	localDownloader.selectNodes = infos
	localDownloader.length = length
	localDownloader.taskID = taskID
	localDownloader.outPath = filepath.Join(lm.dfGetConfig.RV.TargetDir, name)
	localDownloader.downloadAPI = lm.downloadAPI
	localDownloader.superAPI = lm.supernodeAPI
	localDownloader.systemDataDir = lm.dfGetConfig.RV.SystemDataDir


	rd, err = localDownloader.RunStream(ctx)
	if err != nil {
		logrus.Errorf("RunStream failed: %v", err)
	}else {
		return rd, nil
	}

	// try to schedule by super node
superNodeSchedule:

	return lm.scheduleBySuperNode(ctx, url, header, name)
}

func (lm *LocalManager) scheduleBySuperNode(ctx context.Context, url string, header map[string][]string, name string) (io.Reader, error) {
	dfClient := p2p.NewClient(lm.cfg)
	return dfClient.DownloadStreamContext(ctx, url, header, name)
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

//func (lm *LocalManager) tryToDownloadFromPeer(result []*scheduler.Result) (io.Reader, error) {
//	var (
//		lastErr error
//	)
//
//	for _, r := range result {
//		r.StartDownload(r.DstCid, r.Generation)
//		// todo:
//
//		reader, err := lm.downloadFromPeer(r.PeerInfo, r.Pieces[0].Path, r.Task)
//		lastErr = err
//		if err != nil {
//			go r.FinishDownload(r.DstCid, r.Generation)
//			continue
//		}
//
//		return reader, nil
//	}
//
//	return nil, lastErr
//}

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
		ds, err := httputils.GetRangeSE(headerStr, 1024 * 1024 * 16)
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
	nodes, err := lm.fetchP2PNetworkInfo(urls)
	if err != nil {
		logrus.Errorf("failed to fetchP2PNetworkInfo: %v", err)
		return
	}

	lm.sm.SyncSchedulerInfo(nodes)
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