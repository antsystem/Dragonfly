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

package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/config"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/dragonflyoss/Dragonfly/dfdaemon/downloader"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/exception"
)

var (
	// layerReg the regex to determine if it is an image download
	layerReg = regexp.MustCompile("^.+/blobs/sha256.*$")
)

// DFRoundTripper implements RoundTripper for dfget.
// It uses http.fileTransport to serve requests that need to use dfget,
// and uses http.Transport to serve the other requests.
type DFRoundTripper struct {
	Round            *http.Transport
	Round2           http.RoundTripper
	ShouldUseDfget   func(req *http.Request) bool
	Downloader       downloader.Interface
	StreamDownloader downloader.Stream
	ExtremeDownloader downloader.Stream
	config			 *config.Properties

	nWare			 NumericalWare
}

// New returns the default DFRoundTripper.
func New(opts ...Option) (*DFRoundTripper, error) {
	rt := &DFRoundTripper{
		Round:          defaultHTTPTransport(nil),
		Round2:         http.NewFileTransport(http.Dir("/")),
		ShouldUseDfget: NeedUseGetter,
		nWare:          nWare,
	}

	for _, opt := range opts {
		if err := opt(rt); err != nil {
			return nil, errors.Wrap(err, "apply options")
		}
	}

	if rt.StreamDownloader == nil {
		return nil, errors.Errorf("nil downloader")
	}

	return rt, nil
}

// Option is functional config for DFRoundTripper.
type Option func(rt *DFRoundTripper) error

// WithTLS configures TLS config used for http transport.
func WithTLS(cfg *tls.Config) Option {
	return func(rt *DFRoundTripper) error {
		rt.Round = defaultHTTPTransport(cfg)
		return nil
	}
}

func defaultHTTPTransport(cfg *tls.Config) *http.Transport {
	if cfg == nil {
		cfg = &tls.Config{InsecureSkipVerify: true}
	}
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       cfg,
	}
}

// WithDownloader sets the downloader for the roundTripper.
func WithDownloader(d downloader.Interface) Option {
	return func(rt *DFRoundTripper) error {
		rt.Downloader = d
		return nil
	}
}

func WithStreamDownloader(d downloader.Stream) Option {
	return func(rt *DFRoundTripper) error {
		rt.StreamDownloader = d
		return nil
	}
}

func WithExtremeDownloader(d downloader.Stream) Option {
	return func(rt *DFRoundTripper) error {
		rt.ExtremeDownloader = d
		return nil
	}
}

func WithConfig(cfg *config.Properties) Option {
	return func(rt *DFRoundTripper) error {
		rt.config = cfg
		return nil
	}
}

// WithCondition configures how to decide whether to use dfget or not.
func WithCondition(c func(r *http.Request) bool) Option {
	return func(rt *DFRoundTripper) error {
		rt.ShouldUseDfget = c
		return nil
	}
}

// RoundTrip only process first redirect at present
// fix resource release
func (roundTripper *DFRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	ret, resp, err := roundTripper.directReturn(req)
	if ret {
		return resp, err
	}

	ret, resp, err = roundTripper.isNumericalResult(req)
	if ret {
		return resp, err
	}

	if roundTripper.ShouldUseDfget(req) {
		// delete the Accept-Encoding header to avoid returning the same cached
		// result for different requests
		req.Header.Del("Accept-Encoding")
		logrus.Debugf("round trip with dfget: %s", req.URL.String())

		ctx := context.WithValue(req.Context(), "numericalWare", roundTripper.nWare)
		ctx = context.WithValue(ctx, "key", uuid.New())
		if res, err := roundTripper.download(req.WithContext(ctx), req.URL.String()); err == nil || !exception.IsNotAuth(err) {
			return res, err
		}
	}
	logrus.Debugf("round trip directly: %s %s", req.Method, req.URL.String())
	req.Host = req.URL.Host
	req.Header.Set("Host", req.Host)
	res, err := roundTripper.Round.RoundTrip(req)
	return res, err
}

// download uses dfget to download.
func (roundTripper *DFRoundTripper) download(req *http.Request, urlString string) (*http.Response, error) {
	reader, err := roundTripper.downloadByStream(req.Context(), urlString, req.Header, uuid.New())
	if err != nil {
		logrus.Errorf("download fail: %v", err)
		return nil, err
	}

	resp := &http.Response{
		StatusCode: 200,
		//ContentLength: 1048576,
		Body:       NewNumericalReader(reader, time.Now(), req.Context().Value("key").(string), roundTripper.nWare),
	}
	return resp, nil
	// return response, err
}

// downloadByGetter is used to download file by DFGetter.
func (roundTripper *DFRoundTripper) downloadByGetter(ctx context.Context, url string, header map[string][]string, name string) (string, error) {
	logrus.Infof("start download url:%s to %s in repo", url, name)
	return roundTripper.Downloader.DownloadContext(ctx, url, header, name)
}

func (roundTripper *DFRoundTripper) downloadByStream(ctx context.Context, url string, header map[string][]string, name string) (io.Reader, error) {
	taskID := httputils.GetTaskIDFromHeader(url, header, roundTripper.config.Extreme.SpecKeyOfTaskID)
	if taskID != "" {
		logrus.Infof("start download url in extreme mode: %s to %s in repo", url, name)
		return roundTripper.ExtremeDownloader.DownloadStreamContext(ctx, url, header, name)
	}

	logrus.Infof("start download url:%s to %s in repo", url, name)
	return roundTripper.StreamDownloader.DownloadStreamContext(ctx, url, header, name)
}

// needUseGetter is the default value for ShouldUseDfget, which downloads all
// images layers with dfget.
func NeedUseGetter(req *http.Request) bool {
	return req.Method == http.MethodGet && layerReg.MatchString(req.URL.Path)
}

func (roundTripper *DFRoundTripper) directReturn(req *http.Request) (bool, *http.Response, error) {
	if roundTripper.config.Extreme.SpecKeyOfDirectRet != "" &&
		req.Header.Get(roundTripper.config.Extreme.SpecKeyOfDirectRet) != ""{
		return true, &http.Response{
			StatusCode: 200,
			// add a body with no data
			Body: ioutil.NopCloser(bytes.NewReader([]byte{})),
		}, nil
	}

	return false, nil, nil
}

func (roundTripper *DFRoundTripper) isNumericalResult(req *http.Request)  (bool, *http.Response, error)  {
	if req.Header.Get("x-numerical-ware") == "" {
		return false, nil, nil
	}

	var  baseLine int64 = 0
	var msgErr error
	baseLineStr := req.Header.Get("x-numerical-ware-baseline")
	if baseLineStr != "" {
		bl, err := strconv.ParseInt(baseLineStr, 10, 64)
		if err != nil {
			msgErr = fmt.Errorf("base line parse failed: %v", err)
		}

		baseLine = bl
	}

	reset := req.Header.Get("x-numerical-ware-reset")
	if strings.TrimSpace(reset) == "true" {
		defer roundTripper.nWare.Reset()
	}

	rs := roundTripper.nWare.OutputWithBaseLine(baseLine)
	rs.Err = msgErr
	rsData,_ := json.Marshal(rs)

	return true, &http.Response{
		StatusCode: 200,
		Body: ioutil.NopCloser(bytes.NewReader(rsData)),
	}, nil
}
