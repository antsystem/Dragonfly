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
	"fmt"
	"time"

	"github.com/go-check/check"
)

func (suite *seedSuite) TestBlackList(c *check.C) {
	bm := newBlackListManger(time.Millisecond * 500)

	bm.add("aaaa", "id01")
	c.Assert(bm.check("aaaa", "id01"), check.Equals, false)

	// timeout
	time.Sleep(time.Second)
	bm.add("aaaa", "id02")
	c.Assert(bm.check("aaaa", "id01"), check.Equals, true)
	c.Assert(bm.check("aaaa", "id02"), check.Equals, false)

	// blacklist evict
	i := 0
	for i < 101 {
		if i == 50 {
			bm.add("url_0", "id")
		}
		bm.add(fmt.Sprintf("url_%d", i), "id")
		i++
	}
	c.Assert(bm.check("url_0", "id"), check.Equals, false)
	c.Assert(bm.check("url_1", "id"), check.Equals, true)

	// all timeout
	time.Sleep(time.Second)
	i = 0
	for i < 101 {
		c.Assert(bm.check(fmt.Sprintf("url_%d", i), "id"), check.Equals, true)
		i++
	}
}