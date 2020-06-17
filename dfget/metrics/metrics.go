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
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	prometheusHandler http.Handler
)

// Unit represents the type or precision of a metric that is appended to
// the metrics fully qualified name
type Unit string

const (
	namespace = "dragonfly"

	seconds Unit = "seconds"
	total   Unit = "total"
)

func init() {
	prometheusHandler = promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{})
}

// SinceInMicroseconds gets the time since the specified start in microseconds.
func SinceInMicroseconds(start time.Time) float64 {
	return float64(time.Since(start).Nanoseconds() / time.Microsecond.Nanoseconds())
}

// NewLabelSummary return a new SummaryVec
func NewLabelSummary(subsystem, name, help string, labels ...string) *prometheus.SummaryVec {
	return prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        name,
			Help:        help,
			ConstLabels: nil,
		}, labels)
}

// NewLabelCounter return a new CounterVec
func NewLabelCounter(subsystem, name, help string, labels ...string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        fmt.Sprintf("%s_%s", name, total),
			Help:        help,
			ConstLabels: nil,
		}, labels)
}

// NewLabelGauge return a new GaugeVec
func NewLabelGauge(subsystem, name, help string, labels ...string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        fmt.Sprintf("%s_%s", name, Unit("info")),
			Help:        help,
			ConstLabels: nil,
		}, labels)
}

// NewLabelTimer return a new HistogramVec
func NewLabelTimer(subsystem, name, help string, labels ...string) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        fmt.Sprintf("%s_%s", name, seconds),
			Help:        help,
			ConstLabels: nil,
		}, labels)
}

// GetPrometheusRegistry return a resigtry of Prometheus.
func GetPrometheusRegistry() prometheus.Registerer {
	return prometheus.DefaultRegisterer
}

func GetPrometheusHandler() http.Handler {
	return prometheusHandler
}
