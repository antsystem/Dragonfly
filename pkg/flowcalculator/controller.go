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
	"context"
	"fmt"
	"sync"
	"time"
)

var (
	DefaultController = NewFlowCalculatorController()
)

type CalculateDurationCallBackInterface interface {
	CalculateDurationCallBack(ava float64, sumData int64, duration time.Duration)
}

type flowCalculatorWrapper struct {
	fc           *FlowCalculator
	cbIf         CalculateDurationCallBackInterface
	timeInterval time.Duration
	cancel       func()
}

type FlowCalculatorController struct {
	lock   sync.RWMutex
	set    map[string]*flowCalculatorWrapper
	ctx    context.Context
	cancel func()
}

func NewFlowCalculatorController() *FlowCalculatorController {
	ctx, cancel := context.WithCancel(context.Background())
	return &FlowCalculatorController{
		set:    make(map[string]*flowCalculatorWrapper),
		ctx:    ctx,
		cancel: cancel,
	}
}

// Stop stops the loops of registered flowCalculator.
func (fcc *FlowCalculatorController) Stop() {
	if fcc.cancel != nil {
		fcc.cancel()
	}
}

// NewFlowCalculator generates a flow calculator with specific name, if name is registered, return true as alreadyExist.
func (fcc *FlowCalculatorController) NewFlowCalculator(name string, cbIf CalculateDurationCallBackInterface, cbTimeInterval time.Duration) (alreadyExist bool) {
	fcc.lock.Lock()
	defer fcc.lock.Unlock()

	_, exist := fcc.set[name]
	if exist {
		return true
	}

	ctx, cancel := context.WithCancel(fcc.ctx)

	fw := &flowCalculatorWrapper{
		cbIf:         cbIf,
		fc:           NewFlowCalculator(),
		timeInterval: cbTimeInterval,
		cancel:       cancel,
	}

	fcc.set[name] = fw
	go fcc.calculateDurationCallBackLoop(ctx, fw)
	return false
}

// DeleteFlowCalculator unregister and stop flow calculator by specific name.
func (fcc *FlowCalculatorController) DeleteFlowCalculator(name string) {
	fcc.lock.Lock()
	defer fcc.lock.Unlock()

	fw, exist := fcc.set[name]
	if !exist {
		return
	}

	if fw.cancel != nil {
		fw.cancel()
	}

	delete(fcc.set, name)
}

// GetFlowCalculators gets FlowCalculator by name.
func (fcc *FlowCalculatorController) GetFlowCalculators(name ...string) ([]*FlowCalculator, error) {
	fcc.lock.RLock()
	defer fcc.lock.RUnlock()

	ret := []*FlowCalculator{}
	for _, n := range name {
		fw, exist := fcc.set[n]
		if !exist {
			return ret, fmt.Errorf("name %s has not been registered", n)
		}
		ret = append(ret, fw.fc)
	}

	return ret, nil
}

// calculateDurationCallBackLoop runs a loop which calls the CalculateDurationCallBack with a period defined by cbTimeInterval.
func (fcc *FlowCalculatorController) calculateDurationCallBackLoop(ctx context.Context, fw *flowCalculatorWrapper) {
	ticker := time.NewTicker(fw.timeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			fw.cbIf.CalculateDurationCallBack(fw.fc.CalculateDuration(true))
		}
	}
}
