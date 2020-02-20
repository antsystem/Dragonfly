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

package gc

import (
	"context"
	"github.com/sirupsen/logrus"
	"time"
)

func (gcm *Manager) gcRtTaskPeers(ctx context.Context) {
	peerIds := gcm.rtTaskMgr.ScanDownPeers(ctx)
	logrus.Infof("gc peers %v", peerIds)
	for _, peerId := range peerIds {
		gcm.rtTaskMgr.DeRegisterPeer(ctx, peerId)
	}
}

func (gcm *Manager) gcUrls(ctx context.Context) {
	urls := gcm.rtTaskMgr.ScanExpiredUrls(ctx)
	logrus.Infof("gc urls %v", urls)
	for _, url := range urls {
		gcm.rtTaskMgr.CleanUrlInfo(ctx, url)
	}
}

func (gcm *Manager) gcRtTasks(ctx context.Context) {
	start := time.Now()
	taskIds := gcm.rtTaskMgr.ScanExpiredTasks(ctx)
	for _, taskId := range taskIds {
		gcm.rtTaskMgr.EvictTask(ctx, taskId)
	}
	logrus.Infof("gc %d tasks, take %.1f s", len(taskIds), time.Since(start).Seconds())
}