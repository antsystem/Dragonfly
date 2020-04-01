package scheduler

import (
	"github.com/dragonflyoss/Dragonfly/apis/types"
)

type Result struct {
	DstCid   string
	Local    bool
	PeerInfo *types.PeerInfo
	Task     *types.TaskInfo
	Path     string

	Generation int64
	// before download, it should be called
	StartDownload func(peerID string, generation int64)

	// after finishing download, it should be called
	FinishDownload func(peerID string, generation int64)
}

type notifySt struct {
	peerID     string
	generation int64
}

type localTaskState struct {
	task *types.TaskFetchInfo
	path string
}
