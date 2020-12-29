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
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/dragonflyoss/Dragonfly/pkg/limitreader"
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// downloader manage the downloading of seed file.
type Downloader interface {
	DownloadToWriterAt(ctx context.Context, rangeStruct httputils.RangeStruct, timeout time.Duration, writeOff int64, writerAt io.WriterAt, rateLimit bool) (length int64, err error)
}

func newLocalDownloader(url string, header map[string][]string, rate *ratelimiter.RateLimiter, copyCache bool, flowCounter func(peerIp string, flows int64), respTimer func(dataSize int64, peerIp string, costs time.Duration)) Downloader {
	return &localDownloader{
		url:         url,
		header:      header,
		rate:        rate,
		copyCache:   copyCache,
		flowCounter: flowCounter,
		respTimer:   respTimer,
	}
}

type localDownloader struct {
	url    string
	header map[string][]string

	// downloader will limit the rate.
	rate *ratelimiter.RateLimiter

	// if copyCache sets, the response body will store to memory cache and transfer to writer
	copyCache bool

	// flowCounter counts the net flow when download from other peer.
	flowCounter func(peerIp string, flows int64)
	// respTimer records the response data and costs time.
	respTimer func(dataSize int64, peerIp string, costs time.Duration)
}

func (ld *localDownloader) DownloadToWriterAt(ctx context.Context, rangeStruct httputils.RangeStruct, timeout time.Duration,
	writeOff int64, writerAt io.WriterAt, rateLimit bool) (length int64, err error) {
	return ld.download(ctx, rangeStruct, timeout, writeOff, writerAt, rateLimit)
}

func (ld *localDownloader) download(ctx context.Context, rangeStruct httputils.RangeStruct, timeout time.Duration,
	writeOff int64, writerAt io.WriterAt, rateLimit bool) (length int64, err error) {
	var (
		written int64
		n       int
		rd    io.Reader
		start = time.Now()
	)

	header := map[string]string{}
	for k, v := range ld.header {
		header[k] = v[0]
	}

	header[config.StrRange] = fmt.Sprintf("bytes=%d-%d", rangeStruct.StartIndex, rangeStruct.EndIndex)
	logrus.Debugf("source download info url:%s, header:%v", ld.url, header)
	resp, err := httputils.HTTPClientDoRequest("", "GET", ld.url, header, timeout)
	if err != nil {
		return 0, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return 0, errortypes.NewHTTPError(resp.StatusCode, "resp code is not 200 or 206")
	}

	expectedLen := rangeStruct.EndIndex - rangeStruct.StartIndex + 1

	defer func() {
		if err == nil {
			if ld.flowCounter != nil {
				ld.flowCounter("source", expectedLen)
			}

			if ld.respTimer != nil {
				ld.respTimer(expectedLen, "source", time.Now().Sub(start))
			}
		}
	}()

	defer resp.Body.Close()
	rd = resp.Body
	if rateLimit {
		rd = limitreader.NewLimitReaderWithLimiter(ld.rate, resp.Body, false)
	}

	if rf, ok := writerAt.(ReadAtFrom); ok {
		n, err = rf.ReadAtFrom(rd, writeOff, expectedLen)
		written = int64(n)
	} else {
		written, err = CopyBufferToWriterAt(writeOff, writerAt, rd)
	}

	if err == io.EOF {
		err = nil
	}

	if err != nil {
		return 0, errors.Wrap(err, fmt.Sprintf("failed to download from [%d,%d]", rangeStruct.StartIndex, rangeStruct.EndIndex))
	}

	if written < expectedLen {
		return 0, errors.Wrap(io.ErrShortWrite, fmt.Sprintf("download from [%d,%d], expecte read %d, but got %d", rangeStruct.StartIndex, rangeStruct.EndIndex, expectedLen, written))
	}

	return written, err
}
