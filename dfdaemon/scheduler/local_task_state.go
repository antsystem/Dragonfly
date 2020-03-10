package scheduler

import "github.com/dragonflyoss/Dragonfly/apis/types"

type localTaskState struct {
	task		*types.TaskFetchInfo
	path		string
}
