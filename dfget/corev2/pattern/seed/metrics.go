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
	subSystem      = "download_from_seed"
	proxySubSystem = "proxy"

	K = 1024
	M = 1024 * 1024
)

var (
	downloadSizeBucket    = []int64{4 * K, 8 * K, 16 * K, 32 * K, 64 * K, 128 * K, 256 * K, 512 * K, 1 * M, 2 * M, 4 * M, 8 * M, 16 * M, 32 * M, 64 * M}
	downloadSizeBucketStr = []string{"4K", "8K", "16K", "32K", "64K", "128K", "256K", "512K", "1M", "2M", "4M", "8M", "16M", "32M", "64M", "+Inf"}

	downloadCostsTimeBucket = []float64{.001, .002, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
)

var (
	// downloadDataTimer records cost time of download data from seed node.
	downloadDataTimer = metricsutils.NewHistogram(subSystem, "data_costs_time", "records cost time of download data from seed node", []string{"size", "peer_ip"}, downloadCostsTimeBucket, nil)

	// downloadFlowCounter records all data flow of download from seed node.
	downloadFlowCounter = metricsutils.NewCounter(subSystem, "data_flow", "records all data flow of download from seed node", []string{"peer_ip"}, nil)

	hitCacheFlowCounter = metricsutils.NewCounter(subSystem, "cache_data_flow", "records all data flow hit cache", nil, nil)

	blackListCounter = metricsutils.NewCounter(subSystem, "black_list_count", "records black list counter", []string{"msg"}, nil)

	cacheCounter = metricsutils.NewGauge(proxySubSystem, "cache_count", "now count of cache", nil, nil)

	cacheSize = metricsutils.NewGauge(proxySubSystem, "cache_size", "now size of cache", nil, nil)

	errCounter = metricsutils.NewCounter(proxySubSystem, "err", "err counter", []string{"err"}, nil)
)

func recordDownloadCostTimer(size int64, peerIp string, duration time.Duration) {
	index := sort.Search(len(downloadSizeBucket), func(i int) bool {
		return size <= downloadSizeBucket[i]
	})
	downloadDataTimer.WithLabelValues(downloadSizeBucketStr[index], peerIp).Observe(duration.Seconds())
}

func recordDownloadFlowCounter(size int64, peerIp string) {
	downloadFlowCounter.WithLabelValues(peerIp).Add(float64(size))
}

func recordHitCacheFlowCounter(size int64) {
	hitCacheFlowCounter.WithLabelValues().Add(float64(size))
}
