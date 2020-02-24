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
	// firstly, try to download direct from source url
	directDownload, err := lm.isDownloadDirectReturnSrc(ctx, url)
	if err != nil {
		logrus.Error(err)
	}

	if directDownload {
		return lm.downloadDirectReturnSrc(ctx, url, header)
	}

	// download from peer by internal schedule


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

func (lm *LocalManager) schedulePeerNode(ctx context.Context, url string, header map[string][]string) (*types2.PeerInfo, ) {

}

// downloadFromPeer download file from peer node.
// param:
// 	taskFileName: target file name
func (lm *LocalManager) downloadFromPeer(peer *types2.PeerInfo, taskFileName string) (io.Reader, error) {

}

func (lm *LocalManager)  {

}

// sync p2p network， this function should called by
func (lm *LocalManager) syncP2PNetworkInfo(urls []string) {

}

func (lm *LocalManager) fetchP2PNetworkInfo(urls []string) ([]*types.Node, error) {
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

func (lm *LocalManager) fetchP2PNetworkFromSupernode(node string, req *types.FetchP2PNetworkInfoRequest) ([]*types.Node, error) {
	var(
		start int = 0
		limit int =100
	)

	result := []*types.Node{}
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