package seed

import (
	"fmt"
	"github.com/go-check/check"
	"math/rand"
	"time"
)

func (suite *SeedTestSuite) TestUintArrayEncodeAndDeCode(c *check.C) {
	arr1 := []uint64{0,1,2,3,4,5,10000}
	data1 := EncodeUintArray(arr1)

	deArr1 := DecodeToUintArray(data1)
	c.Assert(len(deArr1), check.Equals, len(arr1))

	for i := 0; i < len(arr1); i ++ {
		c.Assert(arr1[i], check.Equals, deArr1[i])
	}

	arr2 := make([]uint64, 1000000)
	for  i := 0; i < 1000000; i ++ {
		arr2[i] = rand.Uint64()
	}

	now := time.Now()
	data2 := EncodeUintArray(arr2)
	fmt.Printf("encode 1000000 uint64 array, costs %f seconds\n", time.Now().Sub(now).Seconds())

	now = time.Now()
	deArr2 := DecodeToUintArray(data2)
	fmt.Printf("decode 1000000 uint64 array, costs %f seconds\n", time.Now().Sub(now).Seconds())
	c.Assert(len(deArr2), check.Equals, len(arr2))

	for i := 0; i < len(arr2); i ++ {
		c.Assert(arr2[i], check.Equals, deArr2[i])
	}
}