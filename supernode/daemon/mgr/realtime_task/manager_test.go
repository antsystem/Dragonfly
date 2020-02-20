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
	"testing"
	"github.com/go-check/check"
	"github.com/dragonflyoss/Dragonfly/apis/types"
	"sort"
	"github.com/google/uuid"
	"sync"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/dragonflyoss/Dragonfly/supernode/config"
	"github.com/go-openapi/strfmt"
	"github.com/dragonflyoss/Dragonfly/pkg/digest"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

func init() {
	check.Suite(&RtTaskMgrTestSuite{})
}

type RtTaskMgrTestSuite struct {
	rtTaskMgr 	*Manager
}

func (s *RtTaskMgrTestSuite) SetUpSuite(c *check.C) {
	s.rtTaskMgr, _ = NewManager(&config.Config{BaseProperties: config.NewBaseProperties()})
}

func (mgr *RtTaskMgrTestSuite) TestInvalidTask (c *check.C) {
	c.Check(mgr.rtTaskMgr.IsRtTask(context.Background(), &types.TaskRegisterRequest{}), check.Equals, false)
}

func (mgr *RtTaskMgrTestSuite) TestRegistryTask (c *check.C) {
	request := &types.TaskRegisterRequest{
		CID:         "c01",
		IP:          "192.168.1.1",
		HostName:    "node01",
		Path:        "abc",
		Port:        16543,
		RawURL:      "http://abc.com",
		SuperNodeIP: "10.10.10.10",
		TaskID:      "task01",
		TaskURL:     "http://abc.com",
	}

	c.Check(mgr.rtTaskMgr.IsRtTask(context.Background(), request), check.Equals, true)
	resp, err := mgr.rtTaskMgr.Register(context.Background(), request)
	c.Check(err, check.IsNil)
	c.Check(resp.AsSeed, check.Equals, true)

	rtTaskInfos, err := mgr.rtTaskMgr.GetRtTasksInfo(context.Background(), request.TaskID)
	c.Check(err, check.IsNil)
	c.Check(len(rtTaskInfos), check.Equals, 1)
	c.Check(rtTaskInfos[0].RequestPath, check.Equals, request.Path)
	c.Check(rtTaskInfos[0].P2pInfo.PeerInfo.HostName.String(), check.Equals, request.HostName)
	c.Check(rtTaskInfos[0].TaskInfo.TaskURL, check.Equals, request.TaskURL)

	taskIdSet, err := mgr.rtTaskMgr.GetTasksByUrl(context.Background(), request.TaskURL)
	c.Check(err, check.IsNil)
	c.Check(taskIdSet, check.DeepEquals, []string{request.TaskID})
	// report seed finish
	request.AsSeed = true
	request.TaskID = ""
	resp, err = mgr.rtTaskMgr.Register(context.Background(), request)
	c.Check(err, check.IsNil)
	c.Check(resp.TaskID, check.Equals, digest.Sha256(request.TaskURL))

	// clean
	mgr.rtTaskMgr.DeRegisterPeer(context.Background(), rtTaskInfos[0].P2pInfo.PeerInfo.ID)
	isSuccess := mgr.rtTaskMgr.HasTasks(context.Background(), request.TaskID)
	c.Check(isSuccess, check.Equals, false)
	mgr.rtTaskMgr.CleanUrlInfo(context.Background(), "http://abc.com")
	_, err = mgr.rtTaskMgr.GetTasksByUrl(context.Background(), "http://abc.com")
	c.Check(err, check.NotNil)
}

func (mgr *RtTaskMgrTestSuite) TestMultiTasks (c *check.C) {
	_, err := mgr.rtTaskMgr.Register(context.Background(), &types.TaskRegisterRequest {
		CID:         "c01",
		IP:          "192.168.1.1",
		HostName:    "node01",
		Path:        "abc",
		Port:        16543,
		SuperNodeIP: "10.10.10.10",
		TaskURL:     "http://abc-2.com",
		AsSeed:      true,
	})
	c.Check(err, check.IsNil)

	_, err = mgr.rtTaskMgr.Register(context.Background(), &types.TaskRegisterRequest {
		CID:         "c02",
		IP:          "192.168.1.2",
		HostName:    "node02",
		Path:        "abc",
		Port:        16544,
		SuperNodeIP: "10.10.10.10",
		TaskURL:     "http://abc-2.com",
		AsSeed:      true,
	})
	c.Check(err, check.IsNil)

	_, err = mgr.rtTaskMgr.Register(context.Background(), &types.TaskRegisterRequest {
		CID:         "c03",
		IP:          "192.168.1.3",
		HostName:    "node02",
		Path:        "abc",
		Port:        16544,
		SuperNodeIP: "10.10.10.10",
		TaskURL:     "http://abc-2.com",
		AsSeed:      true,
	})

	_, err = mgr.rtTaskMgr.Register(context.Background(), &types.TaskRegisterRequest {
		CID:         "c01",
		IP:          "192.168.1.1",
		HostName:    "node01",
		Path:        "abc",
		Port:        16543,
		RawURL:      "http://abc.com",
		SuperNodeIP: "10.10.10.10",
		TaskID:      "task02-01",
		TaskURL:     "http://abc-2.com",
	})
	c.Check(err, check.IsNil)

	rtTaskInfos, err := mgr.rtTaskMgr.GetRtTasksInfo(context.Background(), digest.Sha256("http://abc-2.com"))
	c.Check(err, check.IsNil)
	c.Check(len(rtTaskInfos), check.Equals, 3)

	c.Check(rtTaskInfos[0].TaskInfo.AsSeed, check.Equals, true)
	expectPeers := []string { "c01", "c02", "c03" }
	peers := []string { rtTaskInfos[0].P2pInfo.peerId, rtTaskInfos[1].P2pInfo.peerId, rtTaskInfos[2].P2pInfo.peerId }
	sort.Strings(peers)
	c.Check(peers, check.DeepEquals, expectPeers)

	taskIdSet, err := mgr.rtTaskMgr.GetTasksByUrl(context.Background(), "http://abc-2.com")
	c.Check(err, check.IsNil)
	c.Check(len(taskIdSet), check.Equals, 1)
	c.Check(taskIdSet[0], check.Equals, digest.Sha256("http://abc-2.com"))

	mgr.rtTaskMgr.DeRegisterPeer(context.Background(), "c01")
	mgr.rtTaskMgr.DeRegisterPeer(context.Background(), "c02")
	mgr.rtTaskMgr.DeRegisterPeer(context.Background(), "c03")
	mgr.rtTaskMgr.CleanUrlInfo(context.Background(), "http://abc-2.com")
}

func (mgr *RtTaskMgrTestSuite) TestStressRegistry (c *check.C) {
	nrWorker := 128
	urls := []string { "http://abc-1.com", "http://abc-2.com", "http://abc-3.com", "http://abc-4.com" }
	tasks := make([]string, 8192)
	for idx, _ := range tasks {
		tasks[idx] = uuid.New().String()
	}
	idSet := newIdSet()
	for _, id := range tasks {
		idSet.add(id)
	}
	c.Check(idSet.size(), check.Equals, len(tasks))
	testFn := func (ip string, cid string, nTasks int, wg *sync.WaitGroup) {
		request := &types.TaskRegisterRequest{
			CID:         cid,
			IP:          strfmt.IPv4(ip),
			HostName:    "xxx",
			Path:        "abc",
			Port:        16549,
			RawURL:      "http://abc.com",
			TaskURL:     "http://abc.com",
			SuperNodeIP: "10.10.10.10",
		}
		idx := 0
		for idx < nTasks {
			request.TaskID = tasks[idx]
			request.TaskURL = urls[int(idx) % 4]
			if _, err := mgr.rtTaskMgr.Register(context.Background(), request); err != nil {
				logrus.Warnf("%s register %s err", request.CID, tasks[idx])
			}
			idx += 1
		}
		wg.Done()
	}
	var wg sync.WaitGroup
	wg.Add(nrWorker) // 32 hosts
	i := 0
	for i < nrWorker {
		go testFn(fmt.Sprintf("192.168.1.%d", i), fmt.Sprintf("c-%d", i), len(tasks), &wg)
		i += 1
	}
	wg.Wait()
	i = 0
	sum := 0
	for i < nrWorker {
		peerId := fmt.Sprintf("c-%d", i)
		p2pInfo, err := mgr.rtTaskMgr.getP2pInfo(context.Background(), peerId)
		c.Check(err, check.IsNil)
		sum += p2pInfo.Load()
		logrus.Infof("%s Load = %d", peerId, p2pInfo.Load())
		i += 1
	}
	c.Check(sum, check.Equals, mgr.rtTaskMgr.cfg.MaxSeedPerObject * (len(tasks) + 4))

	i = 0
	for i < nrWorker {
		peerId := fmt.Sprintf("c-%d", i)
		_, err := mgr.rtTaskMgr.getP2pInfo(context.Background(), peerId)
		c.Check(err, check.IsNil)
		mgr.rtTaskMgr.DeRegisterPeer(context.Background(), peerId)
		i += 1
	}
	for _, url := range urls {
		mgr.rtTaskMgr.CleanUrlInfo(context.Background(), url)
	}
}

func (mgr *RtTaskMgrTestSuite) TestExpiredTasks(c *check.C) {
	request := &types.TaskRegisterRequest{
		CID:         "c01",
		IP:          "192.168.1.1",
		HostName:    "node01",
		Path:        "abc",
		Port:        16543,
		RawURL:      "http://abc.com",
		SuperNodeIP: "10.10.10.10",
		TaskID:      "task01",
		TaskURL:     "http://abc.com",
	}

	_, err := mgr.rtTaskMgr.Register(context.Background(), request)
	c.Check(err, check.IsNil)
	taskMap, _ := mgr.rtTaskMgr.getTaskMap(context.Background(), request.TaskID)
	taskMap.accessTime = 0
	res := mgr.rtTaskMgr.ScanExpiredTasks(context.Background())
	c.Check(res, check.DeepEquals, []string { request.TaskID })
	mgr.rtTaskMgr.EvictTask(context.Background(), request.TaskID)
	mgr.rtTaskMgr.DeRegisterPeer(context.Background(), request.CID)
	mgr.rtTaskMgr.CleanUrlInfo(context.Background(), request.TaskURL)
}

func (mgr *RtTaskMgrTestSuite) TestExpiredUrls(c *check.C) {
	request := &types.TaskRegisterRequest{
		CID:         "c01",
		IP:          "192.168.1.1",
		HostName:    "node01",
		Path:        "abc",
		Port:        16543,
		RawURL:      "http://abc.com",
		SuperNodeIP: "10.10.10.10",
		TaskID:      "task01",
		TaskURL:     "http://abc.com",
	}

	mgr.rtTaskMgr.CleanUrlInfo(context.Background(), request.TaskURL)
	_, err := mgr.rtTaskMgr.Register(context.Background(), request)
	c.Check(err, check.IsNil)
	urlInfo, _ := mgr.rtTaskMgr.getUrlInfo(context.Background(), request.TaskURL)
	urlInfo.createTime = 0
	res := mgr.rtTaskMgr.ScanExpiredUrls(context.Background())
	c.Check(res, check.DeepEquals, []string { "http://abc.com" })
	mgr.rtTaskMgr.CleanUrlInfo(context.Background(), request.TaskURL)
	_, err = mgr.rtTaskMgr.getUrlInfo(context.Background(), request.TaskURL)
	c.Check(err, check.IsNil)
	mgr.rtTaskMgr.DeRegisterPeer(context.Background(), request.CID)
	mgr.rtTaskMgr.CleanUrlInfo(context.Background(), request.TaskURL)
	_, err = mgr.rtTaskMgr.getUrlInfo(context.Background(), request.TaskURL)
	c.Check(err, check.NotNil)
}

func (mgr *RtTaskMgrTestSuite) TestDownPeers(c *check.C) {
	request := &types.TaskRegisterRequest{
		CID: 		 "c01",
		IP:          "192.168.1.1",
		HostName:    "node01",
		Path:        "abc",
		Port:        16543,
		RawURL:      "http://abc.com",
		SuperNodeIP: "10.10.10.10",
		TaskID:      "task04-01",
		TaskURL:     "http://abc.com",
	}

	_, err := mgr.rtTaskMgr.Register(context.Background(), request)
	c.Check(err, check.IsNil)

	p2pInfo, _ := mgr.rtTaskMgr.getP2pInfo(context.Background(), "c01")
	p2pInfo.hbTime = 0

	result := mgr.rtTaskMgr.ScanDownPeers(context.Background())
	c.Check(len(result), check.Equals, 1)
	c.Check(result[0], check.Equals, "c01")
	mgr.rtTaskMgr.DeRegisterPeer(context.Background(), "c01")
	mgr.rtTaskMgr.CleanUrlInfo(context.Background(), "http://abc.com")
}

func (mgr *RtTaskMgrTestSuite) TestHeartBeat(c *check.C) {
	request := &types.TaskRegisterRequest{
		CID:		 "c01",
		IP:          "192.168.1.1",
		HostName:    "node01",
		Path:        "abc",
		Port:        16543,
		RawURL:      "http://abc.com",
		SuperNodeIP: "10.10.10.10",
		TaskID:      "task04-01",
		TaskURL:     "http://abc.com",
	}
	_, err := mgr.rtTaskMgr.Register(context.Background(), request)
	c.Check(err, check.IsNil)

	p2pInfo, _ := mgr.rtTaskMgr.getP2pInfo(context.Background(), "c01")
	p2pInfo.hbTime = 0

	err = mgr.rtTaskMgr.ReportPeerHealth(context.Background(), "c01")
	c.Check(err, check.IsNil)
	c.Check(p2pInfo.hbTime > 0, check.Equals, true)
	mgr.rtTaskMgr.DeRegisterPeer(context.Background(), "c01")
	mgr.rtTaskMgr.CleanUrlInfo(context.Background(), "http://abc.com")
}