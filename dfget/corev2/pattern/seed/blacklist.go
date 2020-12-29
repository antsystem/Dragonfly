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
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
)

const (
	defaultBlackListTimeout = time.Minute
	blackListSize           = 100
)

type blackList struct {
	sync.Mutex
	peerIDs map[string]time.Time
}

func (b *blackList) add(id string, timeout time.Duration) {
	b.Lock()
	defer b.Unlock()
	blackListCounter.WithLabelValues("INSERT").Inc()
	b.peerIDs[id] = time.Now().Add(timeout)
}

func (b *blackList) exists(id string) bool {
	b.Lock()
	defer b.Unlock()

	if p, ok := b.peerIDs[id]; ok {
		if p.After(time.Now()) {
			blackListCounter.WithLabelValues("HIT").Inc()
			return true
		}
		blackListCounter.WithLabelValues("TIMEOUT").Inc()
		delete(b.peerIDs, id)
	}
	return false
}

type blackListManger struct {
	sync.Mutex
	lists   *lru.Cache
	timeout time.Duration
}

func newBlackListManger(timeout time.Duration) *blackListManger {
	l := lru.New(blackListSize)
	l.OnEvicted = func(key lru.Key, value interface{}) {
		blackListCounter.WithLabelValues("EVICT").Inc()
	}
	if timeout == 0 {
		timeout = defaultBlackListTimeout
	}
	return &blackListManger{
		lists:   l,
		timeout: timeout,
	}
}

func (bm *blackListManger) getBlackList(url string, create bool) *blackList {
	bm.Lock()
	defer bm.Unlock()

	if item, ok := bm.lists.Get(url); ok {
		return item.(*blackList)
	}
	if create {
		b := &blackList{peerIDs: make(map[string]time.Time)}
		bm.lists.Add(url, b)
		return b
	}
	return nil
}

func (bm *blackListManger) add(url, id string) {
	bm.getBlackList(url, true).add(id, bm.timeout)
}

func (bm *blackListManger) check(url, id string) bool {
	if b := bm.getBlackList(url, false); b != nil {
		return !b.exists(id)
	}
	return true
}
