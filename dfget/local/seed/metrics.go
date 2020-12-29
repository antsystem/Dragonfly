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

package seed

import (
	"sort"
	"time"

	"github.com/dragonflyoss/Dragonfly/pkg/metricsutils"
)

const (
	subsystem = "dfdaemon_seed"

	K = 1024
	M = 1024 * 1024
	G = 1024 * 1024
)

var (
	//prefetchTimeBucket    = []float64{0.005, 2, 5, 10, 20, 40, 60, 80, 100, 200, 400}
	prefetchSizeBucket    = []int64{16 * K, 32 * K, 64 * K, 128 * K, 256 * K, 512 * K, 1 * M, 2 * M, 4 * M, 8 * M}
	prefetchSizeBucketStr = []string{"16K", "32K", "64K", "128K", "256K", "512K", "1M", "2M", "4M", "8M", "+Inf"}
)

var (
	// prefetchActionTimer records the cost time of each prefetch action.
	prefetchActionTimer = metricsutils.NewHistogram(subsystem, "prefetch_action_cost_time", "records the cost time of each prefetch action", []string{"size", "peer_ip"}, nil, nil)

	// records the net flow of all prefetch action.
	prefetchFlowCounter = metricsutils.NewCounter(subsystem, "prefetch_flow_total", "records the net flow of all prefetch action", []string{"peer_ip"}, nil)

	gcCounter = metricsutils.NewCounter(subsystem, "gc_seeds_total", "record the count of gc seed", []string{}, nil)

	errCounter = metricsutils.NewCounter(subsystem, "err", "err count", []string{"err"}, nil)
)

func recordPrefetchCostTimer(size int64, peerIp string, duration time.Duration) {
	index := sort.Search(len(prefetchSizeBucket), func(i int) bool {
		return size <= prefetchSizeBucket[i]
	})
	prefetchActionTimer.WithLabelValues(prefetchSizeBucketStr[index], peerIp).Observe(duration.Seconds())
}

func recordPrefetchFlowCounter(peerIp string, flows int64) {
	prefetchFlowCounter.WithLabelValues(peerIp).Add(float64(flows))
}
