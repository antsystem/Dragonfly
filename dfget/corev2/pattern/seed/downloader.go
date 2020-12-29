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
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/dragonflyoss/Dragonfly/dfget/corev2/basic"
	down "github.com/dragonflyoss/Dragonfly/dfget/corev2/downloader"
	"github.com/dragonflyoss/Dragonfly/dfget/corev2/pattern/seed/api"

	"github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/dfget/local/seed"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/sirupsen/logrus"
)

type downloader struct {
	peer          *basic.SchedulePeerInfo
	timeout       time.Duration
	downloaderAPI api.DownloadAPI
}

func NewDownloader(peer *basic.SchedulePeerInfo, timeout time.Duration, downloadAPI api.DownloadAPI) down.Downloader {
	return &downloader{
		peer:          peer,
		timeout:       timeout,
		downloaderAPI: downloadAPI,
	}
}

func (dn *downloader) Download(ctx context.Context, off, size int64) (int64, io.ReadCloser, error) {
	req := &api.DownloadRequest{
		Path:  dn.peer.Path,
		Range: fmt.Sprintf("%d-%d", off, off+size-1),
	}

	resCode := 0

	res, err := dn.downloaderAPI.Download(dn.peer.IP.String(), int(dn.peer.Port), req, dn.timeout)
	if res != nil {
		resCode = res.StatusCode
	}
	logrus.Debugf("download from %s:%d, path %s, resp code: %d, err: %v", dn.peer.IP.String(), dn.peer.Port, dn.peer.Path, resCode, err)

	if err != nil {
		return int64(resCode), nil, err
	}

	if resCode != http.StatusOK && resCode != http.StatusPartialContent {
		errMsg := ""
		if res.Body != nil {
			data, _ := ioutil.ReadAll(res.Body)
			if len(data) > 0 {
				errMsg = string(data)
			}

			res.Body.Close()
		}

		return int64(resCode), nil, fmt.Errorf("res code is %d, errMsg: %s", resCode, errMsg)
	}

	return httputils.HTTPContentLength(res), res.Body, nil
}

type localDownloader struct {
	sd seed.Seed
}

func NewLocalDownloader(sd seed.Seed) down.Downloader {
	return &localDownloader{sd}
}

func (dn *localDownloader) Download(ctx context.Context, off, size int64) (int64, io.ReadCloser, error) {
	off, size, err := dn.sd.CheckRange(off, size)
	if err != nil {
		return 500, nil, err
	}
	rc, err := dn.sd.Download(off, size)
	if err != nil {
		size = 500
	}
	return size, rc, err
}

type sourceDownloader struct {
	url     string
	hd      map[string]string
	timeout time.Duration
}

func NewSourceDownloader(url string, hd map[string]string, timeout time.Duration) down.Downloader {
	return &sourceDownloader{
		url:     url,
		hd:      hd,
		timeout: timeout,
	}
}

// TODO: support other protocol later
func (dn *sourceDownloader) Download(ctx context.Context, off, size int64) (int64, io.ReadCloser, error) {
	retry := 2
	for retry > 0 {
		dn.hd[config.StrRange] = fmt.Sprintf("bytes=%d-%d", off, off+size-1)
		resp, err := httputils.HTTPGetTimeout(dn.url, dn.hd, dn.timeout)
		if err != nil {
			return int64(resp.StatusCode), nil, err
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			return int64(resp.StatusCode), nil, fmt.Errorf("http download err, code: %d", resp.StatusCode)
		}
		retry--
		if resp.StatusCode != http.StatusPartialContent && off > 0 {
			dataLen, _ := strconv.ParseInt(resp.Header.Get(config.StrContentLength), 10, 64)
			resp.Body.Close()
			if dataLen <= off {
				return 500, nil, fmt.Errorf("out of range")
			}
			size = dataLen - off
			// request again
			continue
		}
		dataLen, _ := strconv.ParseInt(resp.Header.Get(config.StrContentLength), 10, 64)
		return dataLen, resp.Body, nil
	}
	return 500, nil, fmt.Errorf("source download err(server may not support range get)")
}
