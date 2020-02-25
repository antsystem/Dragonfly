package localManager

import (
	"time"
)

const(
	defaultReturnSrcInterval = 30 * time.Second
)

type requestState struct {
	// the request url
	url 		string
	// the time when the url firstly requested
	firstTime	time.Time
	// the recent time when the url requested
	recentTime  time.Time

	// is direct to return src url
	directReturnSrc bool

	returnSrcInterval	time.Duration
}

func newRequestState(url string, directReturnSrc bool) *requestState {
	return &requestState{
		url: url,
		firstTime: time.Now(),
		recentTime: time.Now(),
		directReturnSrc: directReturnSrc,
		returnSrcInterval: defaultReturnSrcInterval,
	}
}

func (rs *requestState) copy() *requestState {
	return &requestState{
		url: rs.url,
		firstTime: rs.firstTime,
		recentTime: rs.recentTime,
		directReturnSrc: rs.directReturnSrc,
		returnSrcInterval: rs.returnSrcInterval,
	}
}

func (rs *requestState) updateRecentTime() {
	rs.recentTime = time.Now()
	if rs.directReturnSrc && rs.firstTime.Add(rs.returnSrcInterval).Before(time.Now()) {
		rs.directReturnSrc = false
	}
}

func (rs *requestState) needReturnSrc() bool {
	return rs.directReturnSrc && rs.firstTime.Add(rs.returnSrcInterval).After(time.Now())
}



