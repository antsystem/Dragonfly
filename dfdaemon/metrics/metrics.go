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

package metrics

import (
	"github.com/dragonflyoss/Dragonfly/pkg/metricsutils"
)

const (
	subsystem = "dfdaemon"

	proxyRespModule = "proxy-resp-module"
)

var (
	// RequestActionPerTimer records the time cost of each request action.
	RequestActionPerTimer = metricsutils.NewHistogram(subsystem, "request_cost_time", "records the time cost of each request action", nil, nil, nil)

	// RequestActionFlowSummary records the net flow of each proxy request.
	RequestActionFlowSummary = metricsutils.NewSummary(subsystem, "request_resp_data_flow", "records the net flow of response data for each proxy request", nil, nil, nil)

	// RequestActionCounter records number of proxy request action.
	RequestActionCounter = metricsutils.NewCounter(subsystem, "request_number_total", "records number of proxy request action", nil, nil)

	// RequestAllFlowCounter records the all net flow of response data for request.
	RequestAllFlowCounter = metricsutils.NewCounter(subsystem, "request_resp_data_flow_total", "records the all net flow of response data for request", nil, nil)

	// RequestActionFailureCounter records number of failure proxy request action.
	RequestActionFailureCounter = metricsutils.NewCounter(subsystem, "request_action_failure_total", "records number of failure proxy request action", []string{"url", "range", "errorMsg"}, nil)
	//defaultFlowCalculatorCb = &flowCalculatorCb{}
)

//type flowCalculatorCb struct{}
//
//func (fc *flowCalculatorCb) CalculateDurationCallBack(ava float64, sumData int64, duration time.Duration) {
//	RequestActionBpsTimer.WithLabelValues().Set(ava)
//	RequestAllFlowCounter.WithLabelValues().Add(ava * duration.Seconds())
//}
//
//func FlowCalculatorReadStream(rc io.ReadCloser) io.ReadCloser {
//	flowcalculator.DefaultController.NewFlowCalculator(proxyRespModule, defaultFlowCalculatorCb, time.Second)
//	list, _ := flowcalculator.DefaultController.GetFlowCalculators(proxyRespModule)
//	return flowcalculator.NewFlowCalculatorReadCloser(rc, list...)
//}
