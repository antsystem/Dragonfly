package queue

import (
	"github.com/go-check/check"
)

func (suite *DFGetUtilSuite) TestCircleQueue(c *check.C) {
	q := NewCircleQueue(5)

	q.PutFront("key1", 1)

	v1, err := q.GetItemByKey("key1")
	c.Assert(err, check.IsNil)
	c.Assert(v1.(int), check.Equals, 1)

	items := q.GetFront(1)
	c.Assert(len(items), check.Equals, 1)
	c.Assert(items[0], check.Equals, 1)

	q.PutFront("key2", 2)
	q.PutFront("key1", 3)

	v1, err = q.GetItemByKey("key1")
	c.Assert(err, check.IsNil)
	c.Assert(v1.(int), check.Equals, 3)

	items = q.GetFront(10)
	c.Assert(len(items), check.Equals, 2)
	c.Assert(items[0], check.Equals, 3)
	c.Assert(items[1], check.Equals, 2)

	items = q.GetFront(1)
	c.Assert(len(items), check.Equals, 1)
	c.Assert(items[0], check.Equals, 3)

	_, err = q.GetItemByKey("key3")
	c.Assert(err, check.NotNil)

	q.PutFront("key3", "data3")
	q.PutFront("key4", "data4")
	q.PutFront("key5", "data5")

	items = q.GetFront(10)
	c.Assert(len(items), check.Equals, 5)
	c.Assert(items[0], check.Equals, "data5")
	c.Assert(items[1], check.Equals, "data4")
	c.Assert(items[2], check.Equals, "data3")
	c.Assert(items[3], check.Equals, 3)
	c.Assert(items[4], check.Equals, 2)

	q.PutFront("key6", "data6")
	_, err = q.GetItemByKey("key2")
	c.Assert(err, check.NotNil)

	items = q.GetFront(5)
	c.Assert(len(items), check.Equals, 5)
	c.Assert(items[0], check.Equals, "data6")
	c.Assert(items[1], check.Equals, "data5")
	c.Assert(items[2], check.Equals, "data4")
	c.Assert(items[3], check.Equals, "data3")
	c.Assert(items[4], check.Equals, 3)
}
