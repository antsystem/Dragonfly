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

package seedtask

import (
	"sync"
	"time"

	"github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/sirupsen/logrus"
)

type preheat struct {
	lock  *sync.Mutex
	tasks []*types.PreHeatInfo
}

func (ph *preheat) hasTask(id string) bool {
	ph.lock.Lock()
	defer ph.lock.Unlock()
	for _, p := range ph.tasks {
		if p.SeedTaskID == id {
			return true
		}
	}

	return false
}

func (ph *preheat) addTask(task *types.TaskInfo) {
	ph.lock.Lock()
	defer ph.lock.Unlock()
	ph.tasks = append(ph.tasks, &types.PreHeatInfo{
		Headers:    flattenHeader(task.Headers),
		SeedTaskID: task.ID,
		URL:        task.TaskURL,
	})
}

func (ph *preheat) getAll() []*types.PreHeatInfo {
	ph.lock.Lock()
	defer ph.lock.Unlock()

	res := ph.tasks
	ph.tasks = make([]*types.PreHeatInfo, 0)

	return res
}

func newPreheat() *preheat {
	return &preheat{
		lock:  new(sync.Mutex),
		tasks: make([]*types.PreHeatInfo, 0),
	}
}

type P2pInfo struct {
	peerID   string
	PeerInfo *types.PeerInfo
	fixSeed  bool
	hbTime   int64
	taskIDs  *idSet // seed tasks
	ph       *preheat
}

func (p2p *P2pInfo) Load() int { return p2p.taskIDs.size() }

func (p2p *P2pInfo) addTask(id string) { p2p.taskIDs.add(id) }

func (p2p *P2pInfo) deleteTask(id string) { p2p.taskIDs.delete(id) }

func (p2p *P2pInfo) hasTask(id string) bool { return p2p.taskIDs.has(id) }

func (p2p *P2pInfo) update() { p2p.hbTime = time.Now().Unix() }

// a tuple of TaskInfo and P2pInfo
type SeedInfo struct {
	AllowSeedDownload bool
	RequestPath       string
	TaskInfo          *types.TaskInfo
	P2pInfo           *P2pInfo
}

// point to a real-time task
type SeedMap struct {
	sync.RWMutex
	expireTime time.Time
	/* seed task id */
	taskID     string
	url        string
	fullLength int64
	/* store all task-peer info */
	tasks []*SeedInfo
	/* seed schedule method */
	scheduler seedScheduler
}

func newSeedTaskMap(taskID, url string, maxTaskPeers int) *SeedMap {
	return &SeedMap{
		url:       url,
		taskID:    taskID,
		tasks:     make([]*SeedInfo, maxTaskPeers),
		scheduler: &defaultScheduler{},
	}
}

func headersWithoutRange(h map[string]string) map[string]string {
	ret := make(map[string]string)
	if h == nil {
		return ret
	}
	for k, v := range h {
		if k == "Range" {
			continue
		}
		ret[k] = v
	}
	return ret
}

func getHTTPFileLength(req *types.TaskCreateRequest) (int64, error) {
	resp, err := httputils.HTTPGetTimeout(req.RawURL, headersWithoutRange(req.Headers), time.Second)
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()

	return httputils.HTTPContentLength(resp), nil
}

func (taskMap *SeedMap) tryAddNewTask(peers []*P2pInfo, this *P2pInfo,
	taskRequest *types.TaskCreateRequest, staticPeerMode bool) bool {
	taskMap.Lock()
	defer taskMap.Unlock()

	if taskRequest.FileLength > 0 {
		if taskMap.fullLength <= 0 {
			taskMap.fullLength = taskRequest.FileLength
		}
		if taskMap.fullLength != taskRequest.FileLength {
			logrus.Warnf("Task has different file length, old: %d, now: %d",
				taskMap.fullLength, taskRequest.FileLength)
		}
	}
	if taskMap.fullLength == 0 {
		taskMap.fullLength, _ = getHTTPFileLength(taskRequest)
		logrus.Infof("Get url %s file length %d", taskMap.url, taskMap.fullLength)
	}

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
		AsSeed:         true,
	}
	if taskMap.scheduler.Schedule(
		taskMap.tasks,
		&SeedInfo{
			RequestPath:       taskRequest.Path,
			TaskInfo:          newTaskInfo,
			P2pInfo:           this,
			AllowSeedDownload: false,
		}) {
		if staticPeerMode {
			taskMap.scheduler.Preheat(newTaskInfo, peers, len(taskMap.tasks))
		}
		return true
	}
	return false
}

func (taskMap *SeedMap) listTasks() []*SeedInfo {
	taskMap.RLock()
	defer taskMap.RUnlock()

	result := make([]*SeedInfo, 0)
	for _, v := range taskMap.tasks {
		if v == nil {
			continue
		}
		result = append(result, v)
	}

	return result
}

func (taskMap *SeedMap) update(dur time.Duration) {
	taskMap.expireTime = time.Now().Add(dur)
}

func (taskMap *SeedMap) isExpired() bool {
	return time.Now().After(taskMap.expireTime)
}

func (taskMap *SeedMap) remove(id string) bool {
	taskMap.Lock()
	defer taskMap.Unlock()

	i := 0
	left := len(taskMap.tasks)
	for i < len(taskMap.tasks) {
		if taskMap.tasks[i] == nil {
			left--
		} else if taskMap.tasks[i].P2pInfo.peerID == id {
			taskMap.tasks[i].P2pInfo.deleteTask(taskMap.taskID)
			taskMap.tasks[i] = nil
			left--
		}
		i++
	}

	return left == 0
}

func (taskMap *SeedMap) removeAllPeers() {
	taskMap.Lock()
	defer taskMap.Unlock()

	for idx, task := range taskMap.tasks {
		if task == nil {
			continue
		}
		taskMap.tasks[idx] = nil
		task.P2pInfo.deleteTask(task.TaskInfo.ID)
	}
}
