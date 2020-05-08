package hash_circler

import (
	"github.com/go-check/check"
	"math"
	"math/rand"
	"testing"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

type hashCirclerSuite struct{
	hashMap map[string]uint64
}

func init() {
	check.Suite(&hashCirclerSuite{
		hashMap: make(map[string]uint64),
	})
}

func (suite *hashCirclerSuite) registerHashKV(key string, value uint64) {
	suite.hashMap[key] = value
}

func (suite *hashCirclerSuite) unRegisterHashKV(key string) {
	delete(suite.hashMap, key)
}

func (suite *hashCirclerSuite) cleanHashMap() {
	suite.hashMap = make(map[string]uint64)
}

func (suite *hashCirclerSuite) hash(input string) uint64 {
	v, ok := suite.hashMap[input]
	if ok {
		return v
	}

	return 0
}

func (suite *hashCirclerSuite) TestHashCircler(c *check.C) {
	defer suite.cleanHashMap()

	rangeSize := uint64(math.MaxUint64 / 5)
	suite.registerHashKV("v1", rand.Uint64() % rangeSize)
	suite.registerHashKV("v2", rand.Uint64() % rangeSize)
	suite.registerHashKV("v3", rand.Uint64() % rangeSize + rangeSize)
	suite.registerHashKV("v4", rand.Uint64() % rangeSize + rangeSize)
	suite.registerHashKV("v5", rand.Uint64() % rangeSize + rangeSize * 2)
	suite.registerHashKV("v6", rand.Uint64() % rangeSize + rangeSize * 2)
	suite.registerHashKV("v7", rand.Uint64() % rangeSize + rangeSize * 3)
	suite.registerHashKV("v8", rand.Uint64() % rangeSize + rangeSize * 3)
	suite.registerHashKV("v9", rand.Uint64() % rangeSize + rangeSize * 4)
	suite.registerHashKV("v10", rand.Uint64() % rangeSize + rangeSize * 4)

	arr := []string{
		"key1", "key2", "key3", "key4", "key5",
	}

	inputStrs := []string{
		"v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10",
	}

	hasher, err := NewPreSetHashCircler(arr, suite.hash)
	c.Assert(err, check.IsNil)

	for i := 0; i < 5; i ++ {
		k, err := hasher.Hash(inputStrs[i * 2])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[i])

		k, err = hasher.Hash(inputStrs[i * 2 + 1])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[i])
	}

	hasher.Disable(arr[0])
	k, err := hasher.Hash(inputStrs[0])
	c.Assert(err, check.IsNil)
	c.Assert(k, check.Equals, arr[4])

	k, err = hasher.Hash(inputStrs[1])
	c.Assert(err, check.IsNil)
	c.Assert(k, check.Equals, arr[4])

	err = hasher.Enable(arr[0])
	c.Assert(err, check.IsNil)

	for i := 0; i < 5; i ++ {
		k, err := hasher.Hash(inputStrs[i * 2])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[i])

		k, err = hasher.Hash(inputStrs[i * 2 + 1])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[i])
	}

	hasher.Disable(arr[1])
	hasher.Disable(arr[2])
	hasher.Disable(arr[4])

	for i := 0; i < 6 ; i ++ {
		k, err = hasher.Hash(inputStrs[i])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[0])
	}

	for i := 6; i < 10; i ++ {
		k, err = hasher.Hash(inputStrs[i])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[3])
	}

	err = hasher.Enable(arr[1])
	c.Assert(err, check.IsNil)
	for i := 0; i < 2 ; i ++ {
		k, err = hasher.Hash(inputStrs[i])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[0])
	}

	for i := 2; i < 6 ; i ++ {
		k, err = hasher.Hash(inputStrs[i])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[1])
	}

	for i := 6; i < 10; i ++ {
		k, err = hasher.Hash(inputStrs[i])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[3])
	}

	err = hasher.Enable(arr[1])
	c.Assert(err, check.IsNil)
	err = hasher.Enable(arr[2])
	c.Assert(err, check.IsNil)

	for i := 0; i < 3; i ++ {
		k, err := hasher.Hash(inputStrs[i * 2])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[i])

		k, err = hasher.Hash(inputStrs[i * 2 + 1])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[i])
	}

	for i := 6; i < 10; i ++ {
		k, err = hasher.Hash(inputStrs[i])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[3])
	}

	hasher.Disable(arr[0])
	hasher.Disable(arr[1])
	hasher.Disable(arr[2])
	for i := 0; i < 10 ; i ++ {
		k, err = hasher.Hash(inputStrs[i])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[3])
	}

	hasher.Disable(arr[3])
	for i := 0; i < 10 ; i ++ {
		k, err = hasher.Hash(inputStrs[i])
		c.Assert(err, check.NotNil)
	}

	err = hasher.Enable(arr[0])
	c.Assert(err, check.IsNil)
	for i := 0; i < 10 ; i ++ {
		k, err = hasher.Hash(inputStrs[i])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[0])
	}

	err = hasher.Enable(arr[1])
	for i := 0; i < 2 ; i ++ {
		k, err = hasher.Hash(inputStrs[i])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[0])
	}
	for i := 2; i < 10 ; i ++ {
		k, err = hasher.Hash(inputStrs[i])
		c.Assert(err, check.IsNil)
		c.Assert(k, check.Equals, arr[1])
	}
}
