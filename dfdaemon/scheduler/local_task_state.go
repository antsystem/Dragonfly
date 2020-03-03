package scheduler

import "github.com/dragonflyoss/Dragonfly/apis/types"

type localTaskState struct {
	info		*types.TaskInfo
	path		string
}

func (ts *localTaskState) getPath() string {
	return ts.path
}
