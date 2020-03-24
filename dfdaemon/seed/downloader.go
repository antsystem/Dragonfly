package seed

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/dragonflyoss/Dragonfly/pkg/limitreader"
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
	"io"
	"net/http"
	"time"
)

// downloader manage the downloading of seed file
type downloader interface {
	Download(ctx context.Context, rangeStruct httputils.RangeStruct, timeout time.Duration, writer io.Writer) (length int64, err error)
}

func newLocalDownloader(url string, header map[string][]string, rate *ratelimiter.RateLimiter) downloader  {
	return &localDownloader{
		url: url,
		header: header,
		rate: rate,
	}
}

type localDownloader struct {
	url   string
	header map[string][]string
	// todo: support ssl?

	rate *ratelimiter.RateLimiter
}

func (ld *localDownloader) Download(ctx context.Context, rangeStruct httputils.RangeStruct, timeout time.Duration,
	writer io.Writer) (length int64, err error) {

	header := map[string]string{}
	for k,v := range ld.header {
		header[k] = v[0]
	}

	header[config.StrRange] = fmt.Sprintf("bytes=%d-%d", rangeStruct.StartIndex, rangeStruct.EndIndex)
	resp, err := httputils.HTTPWithHeaders("GET", ld.url, header, timeout, nil)
	if err != nil {
		return 0, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return 0, errortypes.NewHttpError(resp.StatusCode, "resp code is not 200 or 206")
	}

	expectedLen := rangeStruct.EndIndex - rangeStruct.StartIndex + 1

	defer resp.Body.Close()
	rd := limitreader.NewLimitReaderWithLimiter(ld.rate, resp.Body, false)
	written, err := io.CopyN(writer, rd, expectedLen)
	if written < expectedLen {
		return 0, io.ErrShortWrite
	}

	if err == io.EOF {
		err = nil
	}

	return written, err
}
