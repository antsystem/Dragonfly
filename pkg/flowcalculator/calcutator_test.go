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

package flowcalculator

import (
	"bytes"
	"io"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/go-check/check"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

type calculatorSuite struct {
	tmpDir string
}

func init() {
	check.Suite(&calculatorSuite{})
}

func (suite *calculatorSuite) TestFlowCalculatorOperation(c *check.C) {
	start := time.Now()
	fc := NewFlowCalculator()
	fc.Update(10)
	fc.Update(20)

	ava, sum, dur := fc.CalculateDuration(false)
	c.Assert(sum, check.Equals, int64(30))
	expectedAva := float64(sum) / dur.Seconds()
	c.Assert(ava, check.Equals, expectedAva)
	c.Assert(start.Add(dur).After(time.Now()), check.Equals, false)

	fc.Update(30)
	fc.Update(40)

	start1 := time.Now()
	ava, sum, dur = fc.CalculateDuration(true)
	c.Assert(sum, check.Equals, int64(100))
	expectedAva = float64(sum) / dur.Seconds()
	c.Assert(ava, check.Equals, expectedAva)
	c.Assert(start.Add(dur).After(time.Now()), check.Equals, false)

	fc.Update(50)
	fc.Update(50)

	time.Sleep(time.Second)
	ava, sum, dur = fc.CalculateDuration(true)
	c.Assert(sum, check.Equals, int64(200))
	expectedAva = float64(100) / dur.Seconds()
	c.Assert(ava, check.Equals, expectedAva)
	c.Assert(start1.Add(dur).After(time.Now()), check.Equals, false)
	c.Assert(dur.Seconds()-time.Second.Seconds() > 0, check.Equals, true)
}

func (suite *calculatorSuite) TestFlowCalculatorReadCloser(c *check.C) {
	buf := &bytes.Buffer{}
	rc := ioutil.NopCloser(buf)

	start := time.Now()
	fcs := []*FlowCalculator{
		NewFlowCalculator(), NewFlowCalculator(), NewFlowCalculator(),
	}

	crc := NewFlowCalculatorReadCloser(rc, fcs...)
	defer crc.Close()

	_, err := buf.Write([]byte("abcde12345"))
	c.Assert(err, check.IsNil)

	_, err = io.Copy(ioutil.Discard, crc)
	c.Assert(err, check.IsNil)

	time.Sleep(time.Second)

	start1 := time.Now()
	for _, fc := range fcs {
		ava, sum, dur := fc.CalculateDuration(true)
		c.Assert(sum, check.Equals, int64(10))
		expectedAva := float64(10) / dur.Seconds()
		c.Assert(ava, check.Equals, expectedAva)
		c.Assert(start.Add(dur).After(time.Now()), check.Equals, false)
	}

	_, err = buf.Write([]byte("abcde12345"))
	c.Assert(err, check.IsNil)
	_, err = buf.Write([]byte("abcde12345"))
	c.Assert(err, check.IsNil)

	_, err = io.Copy(ioutil.Discard, crc)
	c.Assert(err, check.IsNil)

	for _, fc := range fcs {
		ava, sum, dur := fc.CalculateDuration(true)
		c.Assert(sum, check.Equals, int64(30))
		expectedAva := float64(20) / dur.Seconds()
		c.Assert(ava, check.Equals, expectedAva)
		c.Assert(start1.Add(dur).After(time.Now()), check.Equals, false)
	}
}

type mockCalculateDurationCallBackInstance struct {
	sync.Mutex
	sum      []int64
	ava      []float64
	duration []time.Duration
}

func (mc *mockCalculateDurationCallBackInstance) CalculateDurationCallBack(ava float64, sumData int64, duration time.Duration) {
	mc.Lock()
	defer mc.Unlock()

	mc.sum = append(mc.sum, sumData)
	mc.ava = append(mc.ava, ava)
	mc.duration = append(mc.duration, duration)
}

func (mc *mockCalculateDurationCallBackInstance) ArrayLen() int {
	mc.Lock()
	defer mc.Unlock()

	return len(mc.sum)
}

func (mc *mockCalculateDurationCallBackInstance) GetInfoByIndex(index int) (sum int64, ava float64, dur time.Duration) {
	mc.Lock()
	defer mc.Unlock()

	if index >= len(mc.sum) {
		return 0, 0, 0
	}

	return mc.sum[index], mc.ava[index], mc.duration[index]
}

type mockBufferRc struct {
	buf *bytes.Buffer
}

var c10Bytes = []byte("0123456789")

func (mb *mockBufferRc) WriteData(n int64) {
	counts := int(n / 10)
	left := n % 10

	for i := 0; i < counts; i++ {
		mb.buf.Write(c10Bytes)
	}

	mb.buf.Write(c10Bytes[:left])
}

func (mb *mockBufferRc) Read(p []byte) (n int, e error) {
	return mb.buf.Read(p)
}

func (mb *mockBufferRc) Close() error {
	return nil
}

func (suite *calculatorSuite) TestCalculatorController(c *check.C) {
	names := []string{
		"instance1", "instance2", "instance3",
	}

	cbIfs := []*mockCalculateDurationCallBackInstance{
		{},
		{},
		{},
	}

	for i := range names {
		exist := DefaultController.NewFlowCalculator(names[i], cbIfs[i], time.Second)
		c.Assert(exist, check.Equals, false)
	}

	list, err := DefaultController.GetFlowCalculators(names...)
	c.Assert(err, check.IsNil)
	c.Assert(len(list), check.Equals, 3)

	mbrc := &mockBufferRc{
		buf: &bytes.Buffer{},
	}
	crc := NewFlowCalculatorReadCloser(mbrc, list...)

	mbrc.WriteData(10)
	_, err = io.Copy(ioutil.Discard, crc)
	c.Check(err, check.IsNil)

	// after 1500 ms, the first time to collect duration data should be done.
	time.Sleep(time.Millisecond * 1500)

	for _, cb := range cbIfs {
		l := cb.ArrayLen()
		c.Assert(l, check.Equals, 1)

		sum, ava, dur := cb.GetInfoByIndex(0)
		// duration should be in [1-0.1, 1+0.1]
		c.Assert(dur.Seconds()+0.1 > time.Second.Seconds(), check.Equals, true)
		c.Assert(dur.Seconds()-0.1 < time.Second.Seconds(), check.Equals, true)

		c.Check(sum, check.Equals, int64(10))
		expectedAva := float64(10) / dur.Seconds()
		c.Assert(ava, check.Equals, expectedAva)
	}

	mbrc.WriteData(100)
	_, err = io.CopyN(ioutil.Discard, crc, 40)
	c.Check(err, check.IsNil)

	// after 1000 ms, the second time to collect duration data should be done.
	time.Sleep(time.Millisecond * 1000)

	for _, cb := range cbIfs {
		l := cb.ArrayLen()
		c.Assert(l, check.Equals, 2)

		sum, ava, dur := cb.GetInfoByIndex(1)
		// duration should be in [1-0.1, 1+0.1]
		c.Assert(dur.Seconds()+0.1 > time.Second.Seconds(), check.Equals, true)
		c.Assert(dur.Seconds()-0.1 < time.Second.Seconds(), check.Equals, true)

		c.Check(sum, check.Equals, int64(50))
		expectedAva := float64(40) / dur.Seconds()
		c.Assert(ava, check.Equals, expectedAva)
	}

	_, err = io.Copy(ioutil.Discard, crc)
	c.Check(err, check.IsNil)

	// after 5000 ms, the third time to collect duration data should be done.
	// and the duration data which is collected after third time should be nil.
	time.Sleep(5 * time.Second)

	for i, cb := range cbIfs {
		l := cb.ArrayLen()
		c.Assert(l > 4, check.Equals, true)

		sum, ava, dur := cb.GetInfoByIndex(2)
		// duration should be in [1-0.1, 1+0.1]
		c.Assert(dur.Seconds()+0.1 > time.Second.Seconds(), check.Equals, true)
		c.Assert(dur.Seconds()-0.1 < time.Second.Seconds(), check.Equals, true)

		c.Check(sum, check.Equals, int64(110))
		expectedAva := float64(60) / dur.Seconds()
		c.Assert(ava, check.Equals, expectedAva)

		for j := 3; j < l; j++ {
			sum, ava, dur = cb.GetInfoByIndex(j)
			c.Check(sum, check.Equals, int64(110))
			c.Check(ava, check.Equals, float64(0))
			c.Assert(dur.Seconds()+0.1 > time.Second.Seconds(), check.Equals, true)
			c.Assert(dur.Seconds()-0.1 < time.Second.Seconds(), check.Equals, true)
		}

		// unregister names[i]
		DefaultController.DeleteFlowCalculator(names[i])
	}

	mbrc.WriteData(100)
	_, err = io.CopyN(ioutil.Discard, crc, 100)
	c.Check(err, check.IsNil)
	time.Sleep(5 * time.Second)

	// collect duration data should be stopped, and sum should not be increased.
	for _, cb := range cbIfs {
		l := cb.ArrayLen()
		c.Assert(l > 4, check.Equals, true)

		sum, _, _ := cb.GetInfoByIndex(l - 1)
		c.Check(sum, check.Equals, int64(110))
	}
}
