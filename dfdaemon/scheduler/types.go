package scheduler

import "github.com/dragonflyoss/Dragonfly/apis/types"

type taskInfoWrapper struct {
	task	*types.TaskInfo
	node	*types.Node
}

type Result struct {
	DstCid string
	PeerInfo *types.PeerInfo
	Task     *types.TaskInfo
	Pieces []*types.PieceInfo

	Generation int64
	// before download, it should be called
	StartDownload func(peerID string, generation int64)

	// after finishing download, it should be called
	FinishDownload func(peerID string, generation int64)
}

type notifySt struct {
	peerID   string
	generation int64
}