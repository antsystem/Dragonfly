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
	"sort"

	"github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/sirupsen/logrus"
)

const (
	maxPreHeatNum = 10
)

type seedScheduler interface {
	// try to schedule a new seed
	Schedule(nowTasks []*SeedInfo, newTask *SeedInfo) bool

	// preheat
	Preheat(task *types.TaskInfo, allPeers []*P2pInfo, max int)
}

type defaultScheduler struct{}

func (scheduler *defaultScheduler) Schedule(nowTasks []*SeedInfo, newTask *SeedInfo) bool {
	busyPeer := newTask.P2pInfo
	newTaskInfo := newTask.TaskInfo
	pos := -1
	idx := 0
	nowAvail := 0
	for idx < len(nowTasks) {
		if nowTasks[idx] == nil {
			/* number of seed < MaxSeedPerObj */
			if busyPeer != nil {
				busyPeer = nil
				pos = idx
			}
			idx++
			continue
		}
		p2pInfo := nowTasks[idx].P2pInfo
		taskInfo := nowTasks[idx].TaskInfo
		nowAvail++
		if p2pInfo != nil && p2pInfo.peerID == newTask.P2pInfo.peerID {
			if newTaskInfo.FileLength > 0 && taskInfo.FileLength == 0 {
				// total file has been downloaded
				nowTasks[idx].RequestPath = newTask.RequestPath
				nowTasks[idx].TaskInfo = newTaskInfo
				nowTasks[idx].AllowSeedDownload = true
				logrus.Infof("peer %s has already downloaded %s", p2pInfo.peerID, taskInfo.TaskURL)
				return true
			} else if taskInfo.FileLength > 0 {
				// just update timestamp
				nowTasks[idx].RequestPath = newTask.RequestPath
				nowTasks[idx].AllowSeedDownload = true
				return true
			}
			// Hardly run here
			// This peer already have this task
			logrus.Warnf("peer %s registry same taskid %s twice", p2pInfo.peerID, newTaskInfo.ID)
			return false
		}
		if p2pInfo.Load() < newTask.P2pInfo.Load()+3 {
			idx++
			continue
		}
		if busyPeer != nil && p2pInfo.Load() > busyPeer.Load() {
			busyPeer = p2pInfo
			pos = idx
		}
		idx++
	}
	if pos >= 0 {
		nowTasks[pos] = newTask
		newTask.P2pInfo.addTask(newTaskInfo.ID)
		if nowAvail == 0 {
			newTask.AllowSeedDownload = true
		}
	}
	if busyPeer != nil && busyPeer != newTask.P2pInfo {
		logrus.Infof("seed %s: peer %s up, peer %s down",
			newTask.TaskInfo.TaskURL,
			newTask.P2pInfo.peerID,
			busyPeer.peerID)
		busyPeer.deleteTask(newTaskInfo.ID)
	}
	logrus.Infof("peer %s becomes seed of url %s", newTask.P2pInfo.peerID, newTask.TaskInfo.TaskURL)

	return pos >= 0
}

func (scheduler *defaultScheduler) Preheat(task *types.TaskInfo, allPeers []*P2pInfo, max int) {
	if len(allPeers) == 0 {
		return
	}
	n := int(maxPreHeatNum)
	if n > max {
		n = max
	}
	peers := make([]*P2pInfo, 0)
	for _, p := range allPeers {
		if p.hasTask(task.ID) || p.ph.hasTask(task.ID) {
			n--;
			continue
		}
		peers = append(peers, p)
	}
	if n <= 0 {
		return
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].Load() < peers[j].Load() })
	if n > len(peers) {
		n = len(peers)
	}
	for _, p := range peers[:n] {
		p.ph.addTask(task)
	}
}
