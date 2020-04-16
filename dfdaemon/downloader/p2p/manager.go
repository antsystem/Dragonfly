package p2p

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/scheduler"
	dfgetcfg "github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/sirupsen/logrus"
	"io"
	"math"
	"net/http"
)

// Manager control the
type Manager struct {
	superNodes	 []string
	sm 			 *scheduler.Manager
	supernodeAPI api.SupernodeAPI
	downloadAPI  api.DownloadAPI
	uploaderAPI  api.UploaderAPI
}

func (m *Manager) DownloadStreamContext(ctx context.Context, url string, header map[string][]string, name string) (io.Reader, error) {
	reqRange, err := m.getRangeFromHeader(header)
	if err != nil {
		return nil, err
	}

	logrus.Debugf("start to download stream in seed pattern, url: %s, header: %v, range: [%d, %d]", url,
		header, reqRange.StartIndex, reqRange.EndIndex)

schedule:
	// try to get the peer by internal schedule
	result := m.sm.Scheduler(ctx, url)
	if len(result) == 0 {
		// try to apply to be the seed node
		m.applyForSeedNode(url, header)
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

func (m *Manager) applyForSeedNode(url string, header map[string][]string)  {

}

// sync p2p network to local scheduler.
func (m *Manager) syncP2PNetworkInfo() {

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
