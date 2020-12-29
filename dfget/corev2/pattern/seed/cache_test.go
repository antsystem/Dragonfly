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
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/go-check/check"
)

type mockReadCloser struct{}

func (r *mockReadCloser) Read(b []byte) (int, error) {
	idx := 0
	for idx < len(b) {
		b[idx] = byte(idx & 0xff)
		idx++
	}
	return len(b), nil
}

func (r *mockReadCloser) Close() error {
	return nil
}

type mockErrReadCloser struct{}

func (r *mockErrReadCloser) Read(b []byte) (int, error) {
	return 0, fmt.Errorf("read err")
}

func (r *mockErrReadCloser) Close() error {
	return nil
}

func (suite *seedSuite) TestMemCache(c *check.C) {
	rd := &mockReadCloser{}
	cacheManager := NewDownloadCacheManager(context.Background(), 4096, 1024)

	downloadFn := func(ctx context.Context, hdr map[string][]string, off, size int64) (int64, io.ReadCloser, error) {
		return 0, &mockReadCloser{}, nil
	}

	expect := bytes.NewBuffer(nil)
	buf := make([]byte, 512)
	io.CopyN(expect, rd, 4096)

	// case 1: 0~512
	_, rc, err := cacheManager.StreamContext("1", nil, 0, 512, downloadFn)
	c.Check(err, check.IsNil)
	n, _ := rc.Read(buf)
	c.Check(n, check.Equals, 512)
	c.Check(buf, check.DeepEquals, expect.Bytes()[:512])
	rc.Close()

	// case 2: 800~1312
	_, rc, err = cacheManager.StreamContext("1", nil, 800, 512, downloadFn)
	c.Check(err, check.IsNil)
	bufWriter := bytes.NewBuffer(nil)
	nr, _ := io.Copy(bufWriter, rc)
	c.Check(int(nr), check.Equals, 512)
	c.Check(bufWriter.Bytes(), check.DeepEquals, expect.Bytes()[800:800+512])
	rc.Close()

	// case 3: 0 ~ 81920000
	_, _, err = cacheManager.StreamContext("1", nil, 0, 81920000, downloadFn)
	c.Check(err, check.NotNil)
	c.Check(err.Error(), check.Equals, "too large request")

	// case 4: 16777200 ~ 16777712
	_, rc, err = cacheManager.StreamContext("1", nil, 16777200, 512, downloadFn)
	c.Check(err, check.IsNil)
	b, _ := ioutil.ReadAll(rc)
	c.Check(b, check.DeepEquals, expect.Bytes()[1008:1008+512])
}

func (suite *seedSuite) TestMemCacheDownloadError(c *check.C) {
	cacheManager := NewDownloadCacheManager(context.Background(), 4096, 1024)
	downloadFn := func(ctx context.Context, hdr map[string][]string, off, size int64) (int64, io.ReadCloser, error) {
		return -1, &mockErrReadCloser{}, nil
	}

	_, _, err := cacheManager.StreamContext("1", nil, 0, 512, downloadFn)
	c.Check(err, check.NotNil)
	c.Check(err.Error(), check.Equals, "read err")
}

func (suite *seedSuite) TestMemCacheGc(c *check.C) {
	cacheManager := NewDownloadCacheManager(context.Background(), 4096, 2)
	downloadFn := func(ctx context.Context, hdr map[string][]string, off, size int64) (int64, io.ReadCloser, error) {
		return 0, &mockReadCloser{}, nil
	}

	idx := 0
	for idx < 512 {
		cacheManager.StreamContext(strconv.Itoa(idx), nil, 0, 1, downloadFn)
		idx++
	}
	c.Check(cacheManager.cacheCount(), check.Equals, lruSize)
	idx = 0
	for idx < 100 {
		cacheManager.StreamContext(strconv.Itoa(idx), nil, 0, 200, downloadFn)
		idx++
	}
	time.Sleep(time.Second)
	c.Check(cacheManager.underWater(), check.Equals, true)
}
