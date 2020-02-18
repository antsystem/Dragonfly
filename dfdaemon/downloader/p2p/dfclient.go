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

package p2p

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dragonflyoss/Dragonfly/dfdaemon/config"
	dfgetcfg "github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/dfget/core"
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"github.com/dragonflyoss/Dragonfly/dfget/core/regist"
)

type DFClient struct {
	config       config.DFGetConfig
	dfGetConfig  *dfgetcfg.Config
	supernodeAPI api.SupernodeAPI
	register     regist.SupernodeRegister
	dfClient     core.DFGet
}

type DigestStruct struct {
	Digest string
	httputils.RangeStruct
}

func (c *DFClient) DownloadContext(ctx context.Context, url string, header map[string][]string, name string) (string, error) {
	//	startTime := time.Now()
	dstPath := filepath.Join(c.config.DFRepo, name)
	// r, err := c.doDownload(ctx, url, header, dstPath)
	// if err != nil {
	// 	return "", fmt.Errorf("dfget fail %v", err)
	// }
	// log.Infof("dfget url:%s [SUCCESS] cost:%.3fs", url, time.Since(startTime).Seconds())
	return dstPath, nil
}

func (c *DFClient) DownloadStreamContext(ctx context.Context, url string, header map[string][]string, name string) (io.Reader, error) {
	dstPath := filepath.Join(c.config.DFRepo, name)
	r, err := c.doDownload(ctx, url, header, dstPath)
	if err != nil {
		return nil, fmt.Errorf("dfget fail %v", err)
	}
	return r, nil
}

func convertToDFGetConfig(cfg config.DFGetConfig) *dfgetcfg.Config {
	return &dfgetcfg.Config{
		Nodes:    cfg.SuperNodes,
		DFDaemon: true,
		Pattern:  dfgetcfg.PatternCDN,
		Sign: fmt.Sprintf("%d-%.3f",
			os.Getpid(), float64(time.Now().UnixNano())/float64(time.Second)),
		RV: dfgetcfg.RuntimeVariable{
			LocalIP:  cfg.LocalIP,
			PeerPort: cfg.PeerPort,
		},
	}
}

func NewClient(cfg config.DFGetConfig) *DFClient {
	supernodeAPI := api.NewSupernodeAPI()
	dfGetConfig := convertToDFGetConfig(cfg)
	register := regist.NewSupernodeRegister(dfGetConfig, supernodeAPI)

	client := &DFClient{
		config:       cfg,
		dfGetConfig:  dfGetConfig,
		supernodeAPI: supernodeAPI,
		register:     register,
		dfClient:     core.NewDFGet(),
	}
	client.init()
	return client
}

func (c *DFClient) init() {
	c.dfGetConfig.RV.Cid = getCid(c.dfGetConfig.RV.LocalIP, c.dfGetConfig.Sign)
}

func (c *DFClient) doDownload(ctx context.Context, url string, header map[string][]string, destPath string) (io.Reader, error) {
	runtimeConfig := *c.dfGetConfig
	runtimeConfig.URL = url
	runtimeConfig.RV.TaskURL = url
	runtimeConfig.RV.TaskFileName = getTaskFileName(destPath, c.dfGetConfig.Sign)
	runtimeConfig.Header = flattenHeader(header)
	runtimeConfig.Output = destPath
	runtimeConfig.RV.RealTarget = destPath
	runtimeConfig.RV.TargetDir = filepath.Dir(destPath)

	hr := http.Header(header)
	if digestHeaderStr := hr.Get(dfgetcfg.StrDigest); digestHeaderStr != "" {
		ds, err := getDigest(digestHeaderStr)
		if err != nil {
			return nil, err
		}

		// todo: support the merge request
		if len(ds) != 1 {
			return nil, fmt.Errorf("no support to merge request")
		}

		runtimeConfig.RV.Digest = ds[0].Digest
		runtimeConfig.RV.FileLength = (ds[0].EndIndex - ds[0].StartIndex) + 1
	}

	return c.dfClient.GetReader(ctx, &runtimeConfig)
}

func getTaskFileName(realTarget string, sign string) string {
	return filepath.Base(realTarget) + "-" + sign
}

func getCid(localIP string, sign string) string {
	return localIP + "-" + sign
}

func flattenHeader(header map[string][]string) []string {
	var res []string
	for key, value := range header {
		// discard HTTP host header for backing to source successfully
		if strings.EqualFold(key, "host") {
			continue
		}
		if len(value) > 0 {
			for _, v := range value {
				res = append(res, fmt.Sprintf("%s:%s", key, v))
			}
		} else {
			res = append(res, fmt.Sprintf("%s:%s", key, ""))
		}
	}
	return res
}

func getDigest(digestHeaderStr string) ([]*DigestStruct, error) {
	var (
		digest   string
		rangeStr string
	)

	// digestHeaderStr looks like "sha256_1:0,1000;sha256_2:1001,2000"

	result := []*DigestStruct{}

	arr := strings.Split(digestHeaderStr, ";")
	for _, elem := range arr {
		kv := strings.Split(elem, ":")
		if len(kv) > 3 || len(kv) < 2 {
			return nil, fmt.Errorf("%s is not vaild for digestHeader", digestHeaderStr)
		}

		if len(kv) == 2 {
			digest = fmt.Sprintf("sha256:%s", kv[0])
			rangeStr = kv[1]
		}

		if len(kv) == 3 {
			digest = fmt.Sprintf("%s:%s", kv[0], kv[1])
			rangeStr = kv[2]
		}

		// todo: verify the sha256 string

		rangeIndex := strings.Split(rangeStr, ",")
		if len(rangeIndex) != 2 {
			return nil, fmt.Errorf("%s is not vaild for digestHeader", digestHeaderStr)
		}

		startIndex, err := strconv.ParseInt(rangeIndex[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s is not vaild for digestHeader", digestHeaderStr)
		}

		endIndex, err := strconv.ParseInt(rangeIndex[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s is not vaild for digestHeader", digestHeaderStr)
		}

		ds := &DigestStruct{
			Digest: digest,
			RangeStruct: httputils.RangeStruct{
				StartIndex: startIndex,
				EndIndex:   endIndex,
			},
		}

		result = append(result, ds)
	}

	return result, nil
}
