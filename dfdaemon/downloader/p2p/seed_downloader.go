package p2p

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/sirupsen/logrus"
	"io"
	"path/filepath"
	"time"
)

type SeedDownloader struct {
	selectNodes		[]*downloadNodeInfo

	reqRange        *httputils.RangeStruct
	url				string
	header          map[string][]string

	downloadAPI		api.DownloadAPI
}

func (sd *SeedDownloader) RunStream(ctx context.Context) (io.ReadCloser, error) {
	return sd.tryDownloadByCandidates(ctx)
}

func (sd *SeedDownloader) tryDownloadByCandidates(ctx context.Context) (io.ReadCloser, error) {
	var(
		lastErr  error
	)

	for _, info := range sd.selectNodes {
		rc, _, err := sd.downloadRange(ctx, info)
		if err != nil {
			lastErr = err
			logrus.Errorf("failed to download, url: %v, range: %v, from node %v: %v", sd.url, sd.reqRange, info, err)
			// todo: analysis the error, and if peer node is unavailable, try to update in internal scheduler.
			continue
		}

		return rc, nil
	}

	return nil, lastErr
}

func (sd *SeedDownloader) downloadRange(ctx context.Context, info *downloadNodeInfo) (io.ReadCloser, int, error) {
	var(
		err error
	)

	hd := map[string]string{}

	for k, v := range sd.header {
		hd[k] = v[0]
	}

	req := &api.DownloadRequest{
		Path:  filepath.Join(config.PeerHTTPPathPrefix, info.path),
		PieceRange: fmt.Sprintf("%d-%d", sd.reqRange.StartIndex, sd.reqRange.EndIndex),
		PieceNum: 0,
		PieceSize: int32(sd.reqRange.EndIndex - sd.reqRange.StartIndex + 1),
		Headers: hd,
	}

	// todo: set a suitable timeout
	timeout := time.Duration(0)
	resp, err := sd.downloadAPI.Download(info.ip, info.port, req, timeout)
	if  err != nil {
		return nil, 0, err
	}

	defer func() {
		if err != nil {
			resp.Body.Close()
		}
	}()

	if resp.StatusCode >= 300 {
		return nil, resp.StatusCode, fmt.Errorf("resp code is %d", resp.StatusCode)
	}

	return resp.Body,  resp.StatusCode, nil
}
