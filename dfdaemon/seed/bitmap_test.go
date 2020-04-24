package seed

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"

	"github.com/go-check/check"
)

func (suite *SeedTestSuite) TestBitMap(c *check.C) {
	// bits are in [0, 100 * 64 - 1]
	bm := newBitMap(100, false)
	bm.set(0, 100, true)
	var i int32
	fmt.Printf("bp1, bm[0]: %x, bm[1]: %x\n", bm.bm[0], bm.bm[1])
	for i = 0; i <= 100; i++ {
		rs := bm.get(i, i, true)
		c.Assert(len(rs), check.Equals, 1)
		c.Assert(rs[0].startIndex, check.Equals, i)
		c.Assert(rs[0].endIndex, check.Equals, i)
	}

	var start, end int32

	// random 10000 to set [start, end]
	for i = 0; i <= 1000; i++ {
		n1 := rand.Int31n(101)
		n2 := rand.Int31n(101)
		if n1 < n2 {
			start = n1
			end = n2
		} else {
			start = n2
			end = n1
		}

		rs := bm.get(start, end, true)
		c.Assert(len(rs), check.Equals, 1)
		c.Assert(rs[0].startIndex, check.Equals, start)
		c.Assert(rs[0].endIndex, check.Equals, end)
	}

	bm.set(200, 300, true)
	rs := bm.get(0, 250, true)
	c.Assert(len(rs), check.Equals, 2)
	c.Assert(rs[0].startIndex, check.Equals, int32(0))
	c.Assert(rs[0].endIndex, check.Equals, int32(100))
	c.Assert(rs[1].startIndex, check.Equals, int32(200))
	c.Assert(rs[1].endIndex, check.Equals, int32(250))

	rs = bm.get(0, 250, false)
	c.Assert(len(rs), check.Equals, 1)
	c.Assert(rs[0].startIndex, check.Equals, int32(101))
	c.Assert(rs[0].endIndex, check.Equals, int32(199))

	bm.set(100, 200, false)
	rs = bm.get(0, 250, false)
	c.Assert(len(rs), check.Equals, 1)
	c.Assert(rs[0].startIndex, check.Equals, int32(100))
	c.Assert(rs[0].endIndex, check.Equals, int32(200))

	bm.set(300, 303, true)
	fmt.Printf("index: %d, bp1, bm[4]: %x, bm[5]: %x\n", -1, bm.bm[4], bm.bm[5])

	bm.set(305, 308, true)
	fmt.Printf("index: %d, bp1, bm[4]: %x, bm[5]: %x\n", -2, bm.bm[4], bm.bm[5])

	bm.set(310, 313, true)
	fmt.Printf("index: %d, bp1, bm[4]: %x, bm[5]: %x\n", -3, bm.bm[4], bm.bm[5])

	bm.set(315, 318, true)
	fmt.Printf("index: %d, bp1, bm[4]: %x, bm[5]: %x\n", -4, bm.bm[4], bm.bm[5])

	for i := 0; i < 50; i++ {
		bm.set(int32(300+i*5), int32(300+i*5+3), true)
	}

	rs = bm.get(0, 1000, true)
	c.Check(len(rs), check.Equals, 51)
	c.Check(rs[0].startIndex, check.Equals, int32(0))
	c.Check(rs[0].endIndex, check.Equals, int32(99))
	c.Check(rs[1].startIndex, check.Equals, int32(201))
	c.Check(rs[1].endIndex, check.Equals, int32(303))

	for i := 0; i < 49; i++ {
		c.Check(rs[i+2].startIndex, check.Equals, int32(305+i*5))
		c.Check(rs[i+2].endIndex, check.Equals, int32(308+i*5))
	}
}

func (suite *SeedTestSuite) TestRestoreBitMap(c *check.C) {
	bm := newBitMap(100, false)
	bm.set(0, 10, true)
	bm.set(100, 200, true)

	bmPath := filepath.Join(suite.tmpDir, "TestRestoreBitMap.bits")
	data := bm.encode()

	err := ioutil.WriteFile(bmPath, data, 0644)
	c.Assert(err, check.IsNil)

	readData, err := ioutil.ReadFile(bmPath)
	bm1, err := restoreBitMap(readData)
	c.Assert(err, check.IsNil)
	c.Assert(bm1.maxBitIndex, check.Equals, int32(100 * 64 - 1))
	res := bm1.get(0, 100 * 64 - 1, true)
	c.Assert(len(res), check.Equals, 2)
	c.Assert(res[0].startIndex, check.Equals, int32(0))
	c.Assert(res[0].endIndex, check.Equals, int32(10))
	c.Assert(res[1].startIndex, check.Equals, int32(100))
	c.Assert(res[1].endIndex, check.Equals, int32(200))
}