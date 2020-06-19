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
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
	"time"
)

type DownloaderFactoryCreateOpt struct {
	URL             string
	Header          map[string][]string
	OpenMemoryCache bool
	RateLimiter     *ratelimiter.RateLimiter
	// flowCounter counts the net flow when download from other peer.
	FlowCounter func(peerIp string, flows int64)
	// respTimer records the response data and costs time.
	RespTimer func(dataSize int64, peerIp string, costs time.Duration)
}

// DownloaderFactory creates an instance of Downloader by input.
type DownloaderFactory interface {
	Create(opt DownloaderFactoryCreateOpt) Downloader
}
