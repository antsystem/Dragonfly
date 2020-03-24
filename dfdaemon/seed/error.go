package seed

import (
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"strings"
)

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(strings.ToLower(err.Error()), "timeout")
}

func isHttpError(err error) (code int, msg string, ok bool) {
	if err == nil {
		return 0, "", false
	}

	httpErr, ok := err.(*errortypes.HttpError)
	if !ok {
		return 0, "", false
	}

	return httpErr.Code, httpErr.Msg, true
}
