package p2p

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

	returnSrcInterval	time.Duration
}

func newRequestState(url string) *requestState {
	return &requestState{
		url: url,
		firstTime: time.Now(),
		recentTime: time.Now(),
		returnSrcInterval: defaultReturnSrcInterval,
	}
}

func (rs *requestState) copy() *requestState {
	return &requestState{
		url: rs.url,
		firstTime: rs.firstTime,
		recentTime: rs.recentTime,
		returnSrcInterval: rs.returnSrcInterval,
	}
}

func (rs *requestState) updateRecentTime() {
	rs.recentTime = time.Now()
}
