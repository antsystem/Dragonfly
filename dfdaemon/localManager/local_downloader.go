package localManager

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/transport"
	"github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"github.com/dragonflyoss/Dragonfly/dfget/core/downloader/p2p_downloader"
	"github.com/dragonflyoss/Dragonfly/dfget/types"
	"github.com/dragonflyoss/Dragonfly/pkg/constants"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/dragonflyoss/Dragonfly/pkg/queue"
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
	"io"
	"math"
	"time"

	"github.com/sirupsen/logrus"
)

type downloadNodeInfo struct {
	ip		string
	port	int
	path	string

	peerID	string
	// source url?
	directSource bool
	url 	string
	header  map[string][]string
	// is download from local
	local   bool
	seed    bool
}

// LocalDownloader will download the file, copy to stream and to local file system.
type LocalDownloader struct {
	selectNodes		[]*downloadNodeInfo
	config			*config.Config
	queue			queue.Queue
	clientQueue     queue.Queue

	//taskID			string
	length			int64

	// super node ip, may be ""
	node			string
	url				string
	header          map[string][]string

	outPath			string
	systemDataDir   string
	taskFileName    string

	superAPI		api.SupernodeAPI
	downloadAPI		api.DownloadAPI
	uploaderAPI		api.UploaderAPI

	// postNotifyUploader should be called after notify the local uploader finish
	postNotifyUploader func(req *api.FinishTaskRequest)

	postNotifySeedPrefetch func(ld *LocalDownloader, req *types.RegisterRequest, resp *types.RegisterResponseData)
}

func NewLocalDownloader() *LocalDownloader {
	return &LocalDownloader{
		queue: queue.NewQueue(10),
		clientQueue: queue.NewQueue(10),
	}
}

func (ld *LocalDownloader) RunStream(ctx context.Context) (io.Reader, error) {
	csw := downloader.NewClientStreamWriter(ctx, ld.clientQueue, ld.superAPI, ld.config, true, ld.length)
	go func() {
		err := ld.run(ctx, csw)
		if err != nil {
			logrus.Warnf("P2PDownloader run error: %s", err)
		}
	}()
	return csw, nil
}

func (ld *LocalDownloader) run(ctx context.Context, pieceWriter downloader.PieceWriter) error {
	var (
		lastErr error
	)

	// start PieceWriter
	if err := pieceWriter.PreRun(ctx); err != nil {
		return err
	}
	go func() {
		pieceWriter.Run(ctx)
	}()

	for _, info := range ld.selectNodes {
		ld.processPiece(ctx, info)
		success, err := ld.processItem(ctx, info)
		if !success {
			lastErr = err
			continue
		}

		if success {
			return nil
		}
	}

	//failed to download, notify client stream to reset
	ld.clientQueue.Put("reset")
	ld.clientQueue.Put("last")

	return lastErr
}

func (ld *LocalDownloader) processItem(ctx context.Context, info *downloadNodeInfo) (success bool, err error) {
	for {
		v, ok := ld.queue.PollTimeout(2 * time.Second)
		if ! ok {
			continue
		}

		item := v.(*downloader.Piece)
		if item.Result == constants.ResultFail || item.Result == constants.ResultInvalid {
			// todo: get client error
			return false, fmt.Errorf("failed to download from %s", item.DstCid)
		}

		return true, nil
	}

}

func (ld *LocalDownloader) processPiece(ctx context.Context, info* downloadNodeInfo) {
	logrus.Debugf("pieces to be processed:%v", info)
	pieceTask := &types.PullPieceTaskResponseContinueData{
		Range: fmt.Sprintf("0-%d", ld.length - 1 + config.PieceMetaSize),
		PieceNum: 0,
		PieceSize: int32(ld.length),
		Cid: info.peerID,
		PeerIP: info.ip,
		PeerPort: info.port,
		Path: fmt.Sprintf("%s%s", config.PeerHTTPPathPrefix, info.path),
		Url: info.url,
		Header: info.header,
		//DirectSource: info.directSource,
	}

	// if target is seed, construct the range by header
	if info.seed {
		rangeStr, ok := ld.header[config.StrRange]
		if ok && rangeStr[0] != "" {
			// todo: fix it if no range
			rangeSt, err := httputils.GetRangeSE(rangeStr[0], math.MaxInt64)
			if err == nil {
				pieceTask.Range = fmt.Sprintf("%d-%d", rangeSt[0].StartIndex, rangeSt[0].EndIndex + config.PieceMetaSize)
				pieceTask.PieceSize = int32(rangeSt[0].EndIndex - rangeSt[0].StartIndex + 1)
			}
		}
	}

	go ld.startTask(ctx, pieceTask)
}

// PowerClient will download file content and push content to queue and clientQueue
func (ld *LocalDownloader) startTask(ctx context.Context, data *types.PullPieceTaskResponseContinueData) {
	var(
		nWare transport.NumericalWare
		key   string
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

	powerClientConfig := &downloader.PowerClientConfig{
		//TaskID:  ld.taskID,
		Node: ld.node,
		PieceTask: data,
		Cfg: ld.config,
		Queue: ld.queue,
		ClientQueue: ld.clientQueue,
		RateLimiter: ratelimiter.NewRateLimiter(int64(ld.config.LocalLimit), 2),
		DownloadAPI: ld.downloadAPI,
	}

	powerClient := downloader.NewPowerClient(powerClientConfig)
	if err := powerClient.Run(); err != nil && powerClient.ClientError() != nil {
		//p2p.API.ReportClientError(p2p.node, powerClient.ClientError())
		logrus.Errorf("report client error: %v", powerClient.ClientError())
	}else{
		cost := powerClient.CostReadTime()
		if nWare != nil {
			nWare.Add(key, transport.RemoteIOName, cost.Nanoseconds())
		}
	}
}
