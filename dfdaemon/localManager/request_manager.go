package localManager

import (
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/pkg/errors"
	"time"
)

// requestManager manage the recent the requests, it provides the
type requestManager struct {
	q 	*finiteQueue
}

func newRequestManager() *requestManager {
	return &requestManager{
		q:  newFiniteQueue(100),
	}
}

func (rm *requestManager) addRequest(url string, directReturnSrc bool) error {
	data, err := rm.q.getItemByKey(url)
	if err != nil && err != errortypes.ErrDataNotFound {
		return err
	}

	var rs *requestState = nil
	if err == errortypes.ErrDataNotFound {
		rs = newRequestState(url, directReturnSrc)
	}else {
		ors, ok := data.(*requestState)
		if !ok {
			return errors.Wrapf(errortypes.ErrConvertFailed, "value: %v", data)
		}

		rs = ors.copy()
		rs.updateRecentTime()
		// if directReturnSrc == true, update first time to extend the returnSrc time interval
		if directReturnSrc {
			rs.directReturnSrc = directReturnSrc
			rs.firstTime = time.Now()
		}
	}

	rs.updateRecentTime()
	rm.q.putFront(url, rs)

	return nil
}

// getRecentRequest will return 2 of the recent request url
func (rm *requestManager) getRecentRequest() []string {
	arr := rm.q.getFront(2)
	result := []string{}

	for _, d := range arr {
		if rs, ok := d.(*requestState); ok {
			result = append(result, rs.url)
		}
	}

	return result
}

func (rm *requestManager) getRequestState(url string) (*requestState, error) {
	data, err := rm.q.getItemByKey(url)
	if err != nil {
		return nil, err
	}

	if rs, ok := data.(*requestState); ok {
		return rs, nil
	}

	return nil, errors.Wrapf(errortypes.ErrConvertFailed, "value: %v", data)
}
