package localManager

import (
	"context"
	"fmt"
	types2 "github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/downloader/p2p"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/scheduler"
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	dfgetcfg "github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/dfget/types"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"io"
	"net/http"

	"github.com/sirupsen/logrus"
)

// LocalManager will handle all the request, it will
type LocalManager struct {
	client 	*p2p.DFClient
	sm 		*scheduler.SchedulerManager
	supernodeAPI api.SupernodeAPI
	downloadAPI  api.DownloadAPI
	dfGetConfig  *dfgetcfg.Config

	rm		 *requestManager
}

func NewLocalManager() *LocalManager {
	return &LocalManager{}
}

//
func (lm *LocalManager) DownloadStreamContext(ctx context.Context, url string, header map[string][]string, name string) (io.Reader, error) {

	defer lm.rm.addRequest(url)

	// firstly, try to download direct from source url
	directDownload, err := lm.isDownloadDirectReturnSrc(ctx, url)
	if err != nil {
		logrus.Error(err)
	}

	if directDownload {
		return lm.downloadDirectReturnSrc(ctx, url, header)
	}

	// download from peer by internal schedule
	taskID := lm.getDigestFromHeader(url, header)
	if taskID != "" {
		// local schedule
		result, err := lm.sm.SchedulerByTaskID(ctx, taskID, lm.dfGetConfig.RV.Cid, "", 0)
		if err == nil {

		}
	}

	// try to schedule by super node


	return nil, nil
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

// downloadDirectReturnSrc download file from source url
func (lm *LocalManager) downloadDirectReturnSrc(ctx context.Context, url string, header map[string][]string) (io.Reader, error) {

}

// downloadFromPeer download file from peer node.
// param:
// 	taskFileName: target file name
func (lm *LocalManager) downloadFromPeer(peer *types2.PeerInfo, taskFileName string) (io.Reader, error) {
	lm.downloadAPI.Download(peer.IP, peer.Port, &api.DownloadRequest{Path: taskFileName, })
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