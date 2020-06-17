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
	"sync"

	"github.com/dragonflyoss/Dragonfly/dfget/metrics"
)

const (
	subsystem = "dfdaemon"
)

func init() {
	Register()
}

var (
	// RequestActionTimer records the time cost of each request action.
	RequestActionTimer = metrics.NewLabelTimer(subsystem, "requestActionTimer", "records the time cost of each request action",
		"url", "size")

	// RequestActionCounter records number of request action.
	RequestActionCounter = metrics.NewLabelCounter(subsystem, "requestActionCounter", "records number of request action",
		"url", "size")

	// RequestActionTimer records the bps of each request action.
	RequestActionBpsTimer = metrics.NewLabelSummary(subsystem, "RequestActionBpsTimer", "records the bps of each request action",
		"url", "size")
)

var registerMetrics sync.Once

func Register() {
	registry := metrics.GetPrometheusRegistry()
	registerMetrics.Do(func() {
		registry.MustRegister(RequestActionTimer)
		registry.MustRegister(RequestActionCounter)
		registry.MustRegister(RequestActionBpsTimer)
	})
}
