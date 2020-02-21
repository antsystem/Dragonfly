package scheduler

import (
	"context"
	"sync"
)

// SchedulerManager schedules the peer node to fetch the resource specified by request
type SchedulerManager struct {
	sync.Mutex

	// key is taskID, value is taskState
	taskContainer  *dataMap

	nodeContainer  *dataMap

	// key is the taskURL
	urlContainer  *dataMap


}

func NewScheduler() *SchedulerManager {
	return &SchedulerManager{
		taskContainer: newDataMap(),
	}
}

func (sm *SchedulerManager) Scheduler(ctx context.Context, taskID string, srcCid string) () {

}
