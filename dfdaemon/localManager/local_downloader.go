package localManager

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/downloader/p2p"
	"github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"github.com/dragonflyoss/Dragonfly/dfget/core/downloader/p2p_downloader"
	"github.com/dragonflyoss/Dragonfly/dfget/types"
	"github.com/dragonflyoss/Dragonfly/pkg/constants"
	"github.com/dragonflyoss/Dragonfly/pkg/queue"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/pborman/uuid"
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
}

// LocalDownloader will download the file, copy to stream and to local file system.
type LocalDownloader struct {
	selectNodes		[]*downloadNodeInfo
	config			*config.Config
	queue			queue.Queue
	clientQueue     queue.Queue

	taskID			string
	length			int64

	// super node ip, may be ""
	node			string
	url				string
	header          map[string][]string

	outPath			string
	systemDataDir   string

	superAPI		api.SupernodeAPI
	downloadAPI		api.DownloadAPI
}

func NewLocalDownloader() *LocalDownloader {
	return &LocalDownloader{
		queue: queue.NewQueue(10),
		clientQueue: queue.NewQueue(10),
	}
}

func (ld *LocalDownloader) RunStream(ctx context.Context) (io.Reader, error) {
	csw := downloader.NewClientStreamWriter(ld.clientQueue, ld.superAPI, ld.config, true)
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
		ld.processPiece(info)
		success, err := ld.processItem()
		if !success {
			lastErr = err
			continue
		}

		if success {
			return nil
		}
	}

	return lastErr
}

//
func (ld *LocalDownloader) processItem() (success bool, err error) {
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

		go ld.finishTask(item)
		return true, nil
	}

}

// task is finished, try to download to local file system and report to super node
func (ld *LocalDownloader) finishTask(piece *downloader.Piece) {
	tmpPath := filepath.Join(ld.systemDataDir, uuid.New())
	f, err := os.OpenFile(tmpPath, os.O_TRUNC | os.O_WRONLY | os.O_CREATE, 0664)
	if err != nil {
		logrus.Warnf("failed to open tmp path: %v", err)
		return
	}

	_, err = f.Write(piece.Content.Bytes())
	if err != nil {
		logrus.Warnf("failed to write tmp path: %v", err)
		return
	}

	err = os.Rename(tmpPath, ld.outPath)
	if err != nil {
		logrus.Warnf("failed to rename: %v", err)
		return
	}

	// report to supernode

	req := &types.RegisterRequest{
		RawURL: ld.url,
		TaskURL: ld.url,
		TaskId:  ld.taskID,
		FileLength: ld.length,
		Insecure: ld.config.Insecure,
		Dfdaemon: ld.config.DFDaemon,
		Path: ld.outPath,
		IP: ld.config.RV.LocalIP,
		Port: ld.config.RV.PeerPort,
		Cid: ld.config.RV.Cid,
		Headers: p2p.FlattenHeader(ld.header),
		Md5: ld.config.Md5,
		Identifier: ld.config.Identifier,
	}

	for _, node := range ld.config.Nodes {
		resp, err := ld.superAPI.ReportResource(node, req)
		if err != nil {
			logrus.Error(err)
		}

		if err == nil && resp.IsSuccess() {
			logrus.Infof("success to report resource %v to supernode", req)
			return
		}
	}
}

func (ld *LocalDownloader) processPiece(info* downloadNodeInfo) {
	logrus.Debugf("pieces to be processed:%v", info)
	pieceTask := &types.PullPieceTaskResponseContinueData{
		Range: fmt.Sprintf("0-%d", ld.length - 1),
		PieceNum: 1,
		PieceSize: int32(ld.length),
		Cid: info.peerID,
		PeerIP: info.ip,
		PeerPort: info.port,
		Path: info.path,
		Url: info.url,
		Header: info.header,
		DirectSource: info.directSource,
	}

	go ld.startTask(pieceTask)
}

// PowerClient will download file content and push content to queue and clientQueue
func (ld *LocalDownloader) startTask(data *types.PullPieceTaskResponseContinueData) {
	powerClientConfig := &downloader.PowerClientConfig{
		TaskID:  ld.taskID,
		Node: ld.node,
		PieceTask: data,
		Cfg: ld.config,
		Queue: ld.queue,
		ClientQueue: ld.clientQueue,
		RateLimiter: nil,
		DownloadAPI: ld.downloadAPI,
	}

	powerClient := downloader.NewPowerClient(powerClientConfig)
	if err := powerClient.Run(); err != nil && powerClient.ClientError() != nil {
		//p2p.API.ReportClientError(p2p.node, powerClient.ClientError())
		logrus.Errorf("report client error: %v", powerClient.ClientError())
	}
}

