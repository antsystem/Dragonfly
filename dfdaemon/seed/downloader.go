package seed

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/dragonflyoss/Dragonfly/pkg/limitreader"
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
	"io"
)

// downloader manage the downloading of seed file
type downloader interface {
	Download(ctx context.Context, rangeStruct httputils.RangeStruct, writer io.WriteSeeker) (length int64, err error)
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

func (ld *localDownloader) Download(ctx context.Context, rangeStruct httputils.RangeStruct,
	writer io.WriteSeeker) (length int64, err error) {

	// seek writer
	_, err = writer.Seek(rangeStruct.StartIndex, io.SeekStart)
	if err != nil {
		return 0, err
	}

	header := map[string]string{}
	for k,v := range ld.header {
		header[k] = v[0]
	}

	header[config.StrRange] = fmt.Sprintf("bytes=%d-%d", rangeStruct.StartIndex, rangeStruct.EndIndex)
	resp, err := httputils.HTTPGet(ld.url, header)
	if err != nil {
		return 0, err
	}

	contentLength := resp.ContentLength

	defer resp.Body.Close()
	rd := limitreader.NewLimitReaderWithLimiter(ld.rate, resp.Body, false)
	written, err := io.CopyN(writer, rd, contentLength)
	if written < contentLength {
		return 0, io.ErrShortWrite
	}

	if err == io.EOF {
		err = nil
	}

	return written, err
}
