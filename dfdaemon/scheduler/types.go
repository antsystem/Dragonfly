package scheduler

import "github.com/dragonflyoss/Dragonfly/apis/types"

type taskInfoWrapper struct {
	task	*types.TaskInfo
	node	*types.Node
}

type