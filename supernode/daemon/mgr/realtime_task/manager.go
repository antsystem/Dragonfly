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
	"context"
	dutil "github.com/dragonflyoss/Dragonfly/supernode/daemon/util"
	"github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/sirupsen/logrus"
	"github.com/go-openapi/strfmt"
	"time"
	"github.com/dragonflyoss/Dragonfly/pkg/netutils"
	"github.com/dragonflyoss/Dragonfly/supernode/config"
	"github.com/dragonflyoss/Dragonfly/pkg/digest"
	"fmt"
)

const (
	MIN_AVAIL_SEED_PEERS = 3
	MAX_OUTPUT_TASKS_NUMBER = 15000
)

type TaskRegistryResponce struct {
	TaskID  string
	AsSeed  bool
}

type Manager struct {
	cfg 		 *config.Config
	urlIndex 	 *dutil.Store /* url --> taskid set */
	rtTasksStore *dutil.Store /* taskid --> peerid set */
	p2pInfoStore *dutil.Store
	ipPortMap    *safeMap
}

func NewManager(cfg *config.Config) (*Manager, error) {
	return &Manager{
		cfg:		  cfg,
		urlIndex: 	  dutil.NewStore(),
		rtTasksStore: dutil.NewStore(),
		p2pInfoStore: dutil.NewStore(),
		ipPortMap:    newSafeMap(),
	}, nil
}

func (mgr *Manager) getUrlInfo(ctx context.Context, url string) (*urlInfo, error) {
	item, err := mgr.urlIndex.Get(url)
	if err != nil {
		return nil, err
	}
	urlInfo, _ := item.(*urlInfo)

	return urlInfo, nil
}

func (mgr *Manager) getOrCreateUrlInfo(ctx context.Context, url string) *urlInfo {
	item, err := mgr.urlIndex.Get(url)
	if err != nil {
		item, _ = mgr.urlIndex.LoadOrStore(url, newUrlInfo(url))
	}
	urlInfo, _ := item.(*urlInfo)

	return urlInfo
}

func (mgr *Manager) indexTaskByUrl(ctx context.Context, id string, url string) error {
	urlInfo := mgr.getOrCreateUrlInfo(ctx, url)
	urlInfo.addTask(id)

	return nil
}

func (mgr *Manager) CleanUrlInfo(ctx context.Context, url string) {
	item, err := mgr.urlIndex.Get(url)
	if err != nil {
		return
	}
	shouldRemove := true
	urlInfo, _ := item.(*urlInfo)
	for _, id := range urlInfo.taskSet.list() {
		_, err := mgr.getTaskMap(ctx, id)
		if err != nil {
			urlInfo.deleteTask(id)
			continue
		}
		shouldRemove = false
	}
	if shouldRemove {
		mgr.urlIndex.Delete(url)
	}
}

func (mgr *Manager) getTaskMap(ctx context.Context, taskId string) (*rtTaskMap, error) {
	item, err := mgr.rtTasksStore.Get(taskId)
	if err != nil {
		return nil, err
	}
	rtTaskMap, _ := item.(*rtTaskMap)
	return rtTaskMap, nil
}

func (mgr *Manager) getOrCreateTaskMap(ctx context.Context, taskId string) *rtTaskMap {
	ret, err := mgr.getTaskMap(ctx, taskId)
	if err != nil {
		item, _ := mgr.rtTasksStore.LoadOrStore(taskId,
							newRtTaskMap(taskId, mgr.cfg.MaxSeedPerObject))
		ret, _ = item.(*rtTaskMap)
	}
	return ret
}

func (mgr *Manager) getP2pInfo(ctx context.Context, peerId string) (*P2pInfo, error) {
	item, err := mgr.p2pInfoStore.Get(peerId)
	if err != nil {
		return nil, err
	}
	peerInfo, _ := item.(*P2pInfo)

	return peerInfo, nil
}

func (mgr *Manager) getOrCreateP2pInfo(ctx context.Context, peerId string, peerRequest *types.PeerCreateRequest) *P2pInfo {
	peerInfo, err := mgr.getP2pInfo(ctx, peerId)
	if err != nil {
		newPeerInfo := &types.PeerInfo{
			ID:       peerId,
			IP:       peerRequest.IP,
			Created:  strfmt.DateTime(time.Now()),
			HostName: peerRequest.HostName,
			Port:     peerRequest.Port,
			Version:  peerRequest.Version,
		}
		item, _ := mgr.p2pInfoStore.LoadOrStore(
					peerId,
					&P2pInfo{
						peerId: peerId,
						PeerInfo: newPeerInfo,
						taskIds: newIdSet(),
						rmTaskIds: newIdSet(),
						hbTime: time.Now().Unix()})
		peerInfo, _ = item.(*P2pInfo)
	}
	return peerInfo
}

func convertToCreateRequest(request *types.TaskRegisterRequest, peerId string) *types.TaskCreateRequest {
	return &types.TaskCreateRequest{
		CID:         request.CID,
		CallSystem:  request.CallSystem,
		Dfdaemon:    request.Dfdaemon,
		Headers:     netutils.ConvertHeaders(request.Headers),
		Identifier:  request.Identifier,
		Md5:         request.Md5,
		Path:        request.Path,
		PeerID:      peerId,
		RawURL:      request.RawURL,
		TaskURL:     request.TaskURL,
		SupernodeIP: request.SuperNodeIP,
		TaskID:      request.TaskID,
		FileLength:  request.FileLength,
	}
}

func ipPortToStr(ip strfmt.IPv4, port int32) string {
	return fmt.Sprintf("%s-%d", ip.String(), port)
}

func (mgr *Manager) Register (ctx context.Context, request *types.TaskRegisterRequest) (*TaskRegistryResponce, error) {
	logrus.Debugf("registry rt task %v", request)

	seedState := NOT_SEED
	if request.TaskID == "" {
		request.TaskID = digest.Sha256(request.TaskURL)
		seedState = SEED_READY
	}

	peerCreateReq := &types.PeerCreateRequest{
		IP:       request.IP,
		HostName: strfmt.Hostname(request.HostName),
		Port:     request.Port,
		Version:  request.Version,
	}
	// In real-time situation, cid == peer id
	peerId := request.CID
	p2pInfo := mgr.getOrCreateP2pInfo(ctx, peerId, peerCreateReq)
	// update peer hb time
	p2pInfo.update()
	// check if peer was restarted
	ipPortStr := ipPortToStr(request.IP, request.Port)
	oldPeerId := mgr.ipPortMap.get(ipPortStr)
	if oldPeerId != peerId {
		mgr.DeRegisterPeer(ctx, oldPeerId)
		mgr.ipPortMap.add(ipPortStr, peerId)
	}

	resp := &TaskRegistryResponce{ TaskID: request.TaskID, AsSeed:(seedState != NOT_SEED) }
	if seedState == NOT_SEED {
		urlSHA256 := digest.Sha256(request.TaskURL)
		seedTaskMap := mgr.getOrCreateTaskMap(ctx, urlSHA256)
		if !p2pInfo.taskIds.has(urlSHA256) {
			// never expired
			seedTaskMap.accessTime = -1
			// add a empty task
			if seedTaskMap.tryAddNewTask(p2pInfo, &types.TaskCreateRequest{TaskID:urlSHA256}, SEED_DOWNLOADING) {
				// try becomes seed node successfully
				resp.AsSeed = true
			}
		}
		if seedTaskMap.availTasks >= MIN_AVAIL_SEED_PEERS {
			// seed peers are enough, no need insert new non-blob tasks
			return resp, nil
		}
	}
	rtTaskMap := mgr.getOrCreateTaskMap(ctx, request.TaskID)
	rtTaskMap.update()
	ok := rtTaskMap.tryAddNewTask(p2pInfo, convertToCreateRequest(request, peerId), seedState)
	if ok {
		mgr.indexTaskByUrl(ctx, request.TaskID, request.TaskURL)
	}

	return resp, nil
}

func (mgr *Manager) DeRegisterTask(ctx context.Context, peerId, taskId string) error {
	if !mgr.HasTasks(ctx, taskId) {
		return nil
	}
	taskMap, err := mgr.getTaskMap(ctx, taskId)
	if err != nil {
		return err
	}
	if taskMap.remove(peerId) {
		mgr.rtTasksStore.Delete(taskId)
		logrus.Debugf("Task %s has no peers", taskId)
	}

	return nil
}

func (mgr *Manager) EvictTask(ctx context.Context, taskId string) error {
	taskMap, err := mgr.getTaskMap(ctx, taskId)
	if err != nil {
		return err
	}
	taskMap.removeAllPeers()
	mgr.rtTasksStore.Delete(taskId)

	return nil
}

func (mgr *Manager) DeRegisterPeer(ctx context.Context, peerId string) error {
	if peerId == "" {
		return nil
	}
	logrus.Infof("DeRegister peer %s", peerId)
	p2pInfo, err := mgr.getP2pInfo(ctx, peerId)
	if err != nil {
		logrus.Warnf("No peer %s", peerId)
		return err
	}
	for _, id := range p2pInfo.taskIds.list() {
		mgr.DeRegisterTask(ctx, peerId, id)
	}
	// remove from hash table
	mgr.p2pInfoStore.Delete(peerId)
	mgr.ipPortMap.remove(ipPortToStr(p2pInfo.PeerInfo.IP, p2pInfo.PeerInfo.Port))
	return nil
}

func (mgr *Manager) GetTasksByUrl(ctx context.Context, url string) ([]string, error) {
	urlSha256 := digest.Sha256(url)
	if taskMap, err := mgr.getTaskMap(ctx, urlSha256); err == nil {
		// seed node is enough, no need to scan other tasks
		if taskMap.availTasks >= MIN_AVAIL_SEED_PEERS {
			return []string{ urlSha256 }, nil
		}
	}
	item, err := mgr.urlIndex.Get(url)
	if err != nil {
		return nil, err
	}
	urlInfo, _ := item.(*urlInfo)

	return urlInfo.taskSet.listWithLimit(MAX_OUTPUT_TASKS_NUMBER), nil
}

func (mgr *Manager) GetRtTasksInfo(ctx context.Context, taskId string) ([]*RtTaskInfo, error) {
	taskMap, err := mgr.getTaskMap(ctx, taskId)
	if err != nil {
		return nil, err
	}

	return taskMap.listTasks(), nil
}

func (mgr *Manager) HasTasks(ctx context.Context, taskId string) bool {
	_, err := mgr.rtTasksStore.Get(taskId)

	return err == nil
}

func (mgr *Manager) IsRtTask(ctx context.Context, request *types.TaskRegisterRequest) bool {
	return request.TaskID != "" || request.AsSeed
}

func (mgr *Manager) ReportPeerHealth (ctx context.Context, peerId string) error {
	p2p, err := mgr.getP2pInfo(ctx, peerId)
	if err != nil {
		return err
	}
	p2p.update()

	return nil
}

func (mgr *Manager) ScanDownPeers (ctx context.Context) []string {
	nowTime := time.Now().Unix()

	result := make([]string, 0)
	for _, iter := range mgr.p2pInfoStore.List() {
		p2pInfo, ok := iter.(*P2pInfo)
		if !ok {
			continue
		}
		if nowTime < p2pInfo.hbTime + mgr.cfg.PeerExpireTime {
			continue
		}
		result = append(result, p2pInfo.peerId)
	}

	return result
}

func (mgr *Manager) ScanExpiredUrls(ctx context.Context) []string {
	nowTimeUnix := time.Now().Unix()
	taskExpireTime := int64(mgr.cfg.TaskExpireTime.Seconds())
	result := make([]string, 0)
	for _, iter := range mgr.urlIndex.List() {
		urlInfo, ok := iter.(*urlInfo)
		if !ok {
			continue
		}
		if nowTimeUnix < urlInfo.createTime + taskExpireTime {
			continue
		}
		result = append(result, urlInfo.url)
	}
	return result
}

func (mgr *Manager) ScanExpiredTasks(ctx context.Context) []string {
	nowTime := time.Now().Unix()
	period := int64(mgr.cfg.TaskExpireTime.Seconds())

	result := make([]string, 0)
	allTasks := mgr.rtTasksStore.List()
	for _, iter := range allTasks {
		taskMap, ok := iter.(*rtTaskMap)
		if !ok {
			continue
		}
		if taskMap.accessTime < 0 || nowTime < taskMap.accessTime + period {
			continue
		}
		result = append(result, taskMap.taskId)
	}
	logrus.Infof("total %d tasks, %d expired", len(allTasks), len(result))
	return result
}