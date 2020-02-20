/*
 * Copyright The Dragonfly Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package realtime_task

import (
	"sync"
	"github.com/sirupsen/logrus"
	"github.com/dragonflyoss/Dragonfly/apis/types"
	"time"
)

const (
	SEED_READY = 2
	SEED_DOWNLOADING = 1
	NOT_SEED = 0
)

type urlInfo struct {
	url 		string
	taskSet 	*idSet
	createTime  int64
}

func (urlInfo *urlInfo) addTask(id string) {
	urlInfo.taskSet.add(id)
}

func (urlInfo *urlInfo) deleteTask(id string) {
	urlInfo.taskSet.delete(id)
}

func newUrlInfo(url string) *urlInfo {
	return &urlInfo{
		url: 	 url,
		taskSet: newIdSet(),
		createTime: time.Now().Unix(),
	}
}

type P2pInfo struct {
	peerId	   string
	PeerInfo   *types.PeerInfo
	hbTime	   int64
	taskIds    *idSet // for tmp rt task
	rmTaskIds  *idSet
	seedTask   *idSet
}

func (p2p *P2pInfo) Load() int { return p2p.taskIds.size() }

func (p2p *P2pInfo) addTask(id string) { p2p.taskIds.add(id) }

func (p2p *P2pInfo) deleteTask(id string) { p2p.taskIds.delete(id) }

func (p2p *P2pInfo) update() { p2p.hbTime = time.Now().Unix() }

// a tuple of TaskInfo and P2pInfo
type RtTaskInfo struct {
	RequestPath string
	TaskInfo    *types.TaskInfo
	P2pInfo     *P2pInfo
	SeedState   int
}

// point to a real-time task
type rtTaskMap struct {
	taskId   	string
	lock 	 	*sync.RWMutex
	accessTime  int64
	tasks 	 	[]*RtTaskInfo
	availTasks  int
}

func newRtTaskMap(taskId string, maxTaskPeers int) *rtTaskMap {
	return &rtTaskMap {
		taskId:     taskId,
		tasks:      make([]*RtTaskInfo, maxTaskPeers),
		lock:       new(sync.RWMutex),
		availTasks: 0,
	}
}

func (taskMap *rtTaskMap) tryAddNewTask(p2pInfo *P2pInfo, taskRequest *types.TaskCreateRequest, seedState int) bool {
	taskMap.lock.Lock()
	defer taskMap.lock.Unlock()

	newTaskInfo := &types.TaskInfo{
		ID:             taskRequest.TaskID,
		CdnStatus:      types.TaskInfoCdnStatusSUCCESS,
		FileLength:     taskRequest.FileLength,
		Headers:        taskRequest.Headers,
		HTTPFileLength: taskRequest.FileLength,
		Identifier:     taskRequest.Identifier,
		Md5:            taskRequest.Md5,
		PieceSize:      int32(taskRequest.FileLength),
		PieceTotal:     1,
		RawURL:         taskRequest.RawURL,
		RealMd5:        taskRequest.Md5,
		TaskURL:        taskRequest.TaskURL,
		AsSeed: 		seedState != NOT_SEED,
	}
	return taskMap.tryInsert(
		&RtTaskInfo{
			RequestPath: taskRequest.Path,
			TaskInfo:    newTaskInfo,
			P2pInfo:     p2pInfo,
			SeedState:   seedState,
		}) >= 0
}

func (taskMap *rtTaskMap) listTasks() []*RtTaskInfo {
	taskMap.lock.RLock()
	defer taskMap.lock.RUnlock()

	result := make([]*RtTaskInfo, 0)
	for _, v := range taskMap.tasks {
		if v == nil || v.SeedState == SEED_DOWNLOADING {
			continue
		}
		result = append(result, v)
	}

	return result
}

func (taskMap *rtTaskMap) update() {
	unixNow := time.Now().Unix()
	if unixNow > taskMap.accessTime {
		taskMap.accessTime = unixNow
	}
}

func (taskMap *rtTaskMap) tryInsert(task *RtTaskInfo) int {
	busyPeer := task.P2pInfo
	pos := -1
	idx := 0
	for idx < len(taskMap.tasks) {
		if taskMap.tasks[idx] == nil {
			if busyPeer != nil {
				busyPeer = nil
				pos = idx
			}
			idx += 1
			continue
		}
		p2pInfo := taskMap.tasks[idx].P2pInfo
		if p2pInfo != nil && p2pInfo.peerId == task.P2pInfo.peerId {
			if task.SeedState != taskMap.tasks[idx].SeedState {
				// update seed state
				// use this rt task info instead
				taskMap.tasks[idx] = task
				if task.SeedState == SEED_READY {
					taskMap.availTasks += 1
				}
				return idx
			}
			// Hardly run here
			// This peer already have this task
			logrus.Warnf("peer %s registry same taskid %s twice", p2pInfo.peerId, taskMap.taskId)
			return -1
		}
		if busyPeer != nil && p2pInfo.Load() > busyPeer.Load() {
			busyPeer = p2pInfo
			pos = idx
		}
		idx += 1
	}
	if pos >= 0 {
		if taskMap.tasks[pos] == nil &&
				task.SeedState != SEED_DOWNLOADING {
			taskMap.availTasks += 1
		}
		taskMap.tasks[pos] = task
		task.P2pInfo.addTask(task.TaskInfo.ID)
	}
	if busyPeer != nil && busyPeer != task.P2pInfo {
		busyPeer.deleteTask(taskMap.taskId)
		if task.SeedState != NOT_SEED {
			// tell busy peer to evict task of blob file next heartbeat period
			busyPeer.rmTaskIds.add(taskMap.taskId)
		}
	}

	return pos
}

func (taskMap *rtTaskMap) remove(id string) bool {
	taskMap.lock.Lock()
	defer taskMap.lock.Unlock()

	i := 0
	left := len(taskMap.tasks)
	for i < len(taskMap.tasks) {
		if taskMap.tasks[i] == nil {
			left -= 1
		} else if taskMap.tasks[i].P2pInfo.peerId == id {
			taskMap.tasks[i].P2pInfo.deleteTask(taskMap.taskId)
			taskMap.tasks[i] = nil
			left -= 1
			taskMap.availTasks -= 1
		}
		i += 1
	}

	return left == 0
}

func (taskMap *rtTaskMap) removeAllPeers() {
	taskMap.lock.Lock()
	defer taskMap.lock.Unlock()

	for idx, task := range taskMap.tasks {
		if task == nil {
			continue
		}
		taskMap.tasks[idx] = nil
		task.P2pInfo.deleteTask(task.TaskInfo.ID)
	}
	taskMap.availTasks = 0
}