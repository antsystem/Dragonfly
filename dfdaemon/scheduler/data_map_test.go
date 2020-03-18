package scheduler

import (
	"github.com/dragonflyoss/Dragonfly/apis/types"
	"strings"
	"testing"
	"github.com/go-check/check"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

type dataMapSuite struct{}

func init() {
	check.Suite(&dataMapSuite{})
}

func (suite *dataMapSuite) TestDataMapWithTaskState(c *check.C) {
	d := newDataMap()
	ts1 := &taskState{}

	err := d.add("ts1", ts1)
	c.Assert(err, check.IsNil)

	ret, err := d.getAsTaskState("ts1")
	c.Assert(err, check.IsNil)
	c.Assert(ret, check.Equals, ts1)

	err = d.remove("ts1")
	c.Assert(err, check.IsNil)

	ret, err = d.getAsTaskState("ts1")
	c.Assert(strings.Contains(err.Error(), "empty value"), check.Equals, true)

	err = d.add("ts2", "xxx")
	c.Assert(err, check.IsNil)

	_, err = d.getAsTaskState("ts2")
	c.Assert(strings.Contains(err.Error(), "convert failed"), check.Equals, true)

	err = d.remove("ts2")
	c.Assert(err, check.IsNil)
}

func (suite *dataMapSuite) TestDataMapWithNode(c *check.C) {
	d := newDataMap()
	node1 := &types.Node{}

	err := d.add("node1", node1)
	c.Assert(err, check.IsNil)

	ret, err := d.getAsNode("node1")
	c.Assert(err, check.IsNil)
	c.Assert(ret, check.Equals, node1)

	err = d.remove("node1")
	c.Assert(err, check.IsNil)

	ret, err = d.getAsNode("node1")
	c.Assert(strings.Contains(err.Error(), "empty value"), check.Equals, true)

	err = d.add("node2", "xxx")
	c.Assert(err, check.IsNil)

	_, err = d.getAsNode("node2")
	c.Assert(strings.Contains(err.Error(), "convert failed"), check.Equals, true)

	err = d.remove("node2")
	c.Assert(err, check.IsNil)
}

func (suite *dataMapSuite) TestDataMapWithLocalTaskState(c *check.C) {
	d := newDataMap()
	lts1 := &localTaskState{}

	err := d.add("lts1", lts1)
	c.Assert(err, check.IsNil)

	ret, err := d.getAsLocalTaskState("lts1")
	c.Assert(err, check.IsNil)
	c.Assert(ret, check.Equals, lts1)

	err = d.remove("lts1")
	c.Assert(err, check.IsNil)

	ret, err = d.getAsLocalTaskState("lts1")
	c.Assert(strings.Contains(err.Error(), "empty value"), check.Equals, true)

	err = d.add("lts2", "xxx")
	c.Assert(err, check.IsNil)

	_, err = d.getAsLocalTaskState("lts2")
	c.Assert(strings.Contains(err.Error(), "convert failed"), check.Equals, true)

	err = d.remove("lts2")
	c.Assert(err, check.IsNil)
}
