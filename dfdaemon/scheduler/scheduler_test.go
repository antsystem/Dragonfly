package scheduler

import (
	"context"
	"github.com/dragonflyoss/Dragonfly/apis/types"

	"github.com/go-check/check"
	strfmt "github.com/go-openapi/strfmt"
)

type schedulerSuite struct{}

func init() {
	check.Suite(&schedulerSuite{})
}

func initTaskFetchInfoForTest(id string, asSeed bool, size int64, url string, path string) *types.TaskFetchInfo {
	return &types.TaskFetchInfo{
		Task: &types.TaskInfo{
			ID:         id,
			AsSeed:     asSeed,
			FileLength: size,
			TaskURL:    url,
		},
		Pieces: []*types.PieceInfo{
			{
				Path: path,
			},
		},
	}
}

func initPeerInfoForTest(id string, ip string, port int) *types.PeerInfo {
	return &types.PeerInfo{
		ID:   id,
		IP:   strfmt.IPv4(ip),
		Port: int32(port),
	}
}

func (suite *schedulerSuite) TestNormalScheduler(c *check.C) {
	sm := NewScheduler(&types.PeerInfo{
		ID:   "local_cid",
		IP:   "127.0.0.1",
		Port: 20001,
	})

	node1 := initPeerInfoForTest("node1", "1.1.1.1", 20001)
	node2 := initPeerInfoForTest("node2", "1.1.1.2", 20001)

	task1 := initTaskFetchInfoForTest("task1", true, 100, "http://url1", "seed1")
	task2 := initTaskFetchInfoForTest("task2", true, 200, "http://url2", "seed2")
	task3 := initTaskFetchInfoForTest("task3", true, 300, "http://url3", "seed3")

	nodes := []*types.Node{
		{
			Basic: node1,
			Tasks: []*types.TaskFetchInfo{task1, task2},
			Load:  5,
		},
		{
			Basic: node2,
			Tasks: []*types.TaskFetchInfo{task2, task3},
			Load:  3,
		},
	}

	sm.SyncSchedulerInfo(nodes)
	rs := sm.Scheduler(context.Background(), "http://url1", "", 0)
	c.Assert(len(rs), check.Equals, 1)
	c.Assert(rs[0].PeerInfo.ID, check.Equals, "node1")
	c.Assert(rs[0].Path, check.Equals, "seed1")

	rs = sm.Scheduler(context.Background(), "http://url2", "", 0)
	c.Assert(len(rs), check.Equals, 2)
	c.Assert(rs[0].PeerInfo.ID, check.Equals, "node2")
	c.Assert(rs[0].Path, check.Equals, "seed2")
	c.Assert(rs[1].PeerInfo.ID, check.Equals, "node1")
	c.Assert(rs[1].Path, check.Equals, "seed2")

	rs = sm.Scheduler(context.Background(), "http://url3", "", 0)
	c.Assert(len(rs), check.Equals, 1)
	c.Assert(rs[0].PeerInfo.ID, check.Equals, "node2")
	c.Assert(rs[0].Path, check.Equals, "seed3")

	rs = sm.Scheduler(context.Background(), "http://url4", "", 0)
	c.Assert(len(rs), check.Equals, 0)
}
