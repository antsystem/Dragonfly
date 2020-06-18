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
	"io"
	"sync"
	"time"
)

// FlowCalculator provides the ability to calculate the flow and bps.
type FlowCalculator struct {
	lock sync.RWMutex

	sum int64

	// durationData counts the data from durationStartTime to the time of call CalculateDuration.
	durationData int64

	// durationStartTime records the start time of duration.
	durationStartTime time.Time
}

// NewFlowCalculator generate an instance of FlowCalculator.
func NewFlowCalculator() *FlowCalculator {
	return &FlowCalculator{
		durationStartTime: time.Now(),
	}
}

// Update updates the flow data by adding sum and durationData.
func (c *FlowCalculator) Update(n int64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.sum += n
	c.durationData += n
}

// CalculateDuration counts the data during durationStartTime and now. It computes the bps and sum data.
// if param cleanDuration sets, duration data will be reset.
func (c *FlowCalculator) CalculateDuration(cleanDuration bool) (ava float64, sumData int64, duration time.Duration) {
	var (
		sum               int64
		durationData      int64
		durationStartTime time.Time
	)

	c.lock.Lock()
	now := time.Now()
	sum = c.sum
	durationData = c.durationData
	durationStartTime = c.durationStartTime

	if cleanDuration {
		c.durationData = 0
		c.durationStartTime = now
	}

	c.lock.Unlock()

	// calculate duration data
	costs := now.Sub(durationStartTime)
	ava = float64(durationData) / float64(costs.Seconds())
	sumData = sum

	return ava, sumData, costs
}

// FlowCalculatorReadCloser implements io.ReadCloser, when Read is called, flow will be updated to list of FlowCalculator.
type FlowCalculatorReadCloser struct {
	list []*FlowCalculator
	rc   io.ReadCloser
}

// NewFlowCalculatorReadCloser create an instance of FlowCalculatorReadCloser.
func NewFlowCalculatorReadCloser(rc io.ReadCloser, fc ...*FlowCalculator) *FlowCalculatorReadCloser {
	return &FlowCalculatorReadCloser{
		rc:   rc,
		list: fc,
	}
}

func (crc *FlowCalculatorReadCloser) Update(n int64) {
	for _, f := range crc.list {
		f.Update(n)
	}
}

func (crc *FlowCalculatorReadCloser) Read(p []byte) (n int, err error) {
	n, err = crc.rc.Read(p)
	crc.Update(int64(n))
	return
}

func (crc *FlowCalculatorReadCloser) Close() error {
	return crc.rc.Close()
}
