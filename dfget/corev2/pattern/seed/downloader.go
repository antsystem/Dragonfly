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
	"time"

	"github.com/dragonflyoss/Dragonfly/dfget/corev2/basic"
	down "github.com/dragonflyoss/Dragonfly/dfget/corev2/downloader"
	"github.com/dragonflyoss/Dragonfly/dfget/corev2/pattern/seed/api"

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

func (dn *downloader) Download(ctx context.Context, off, size int64) (io.ReadCloser, error) {
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
		return nil, err
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

		return nil, fmt.Errorf("res code is %d, errMsg: %s", resCode, errMsg)
	}

	return res.Body, nil
}
