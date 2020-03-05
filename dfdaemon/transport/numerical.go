package transport

import (
	"io"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var(
	// signal mode
	nWare NumericalWare
)

func init()  {
	nWare = NewNumericalWare()
}

const(
	TotalName = "total"
	ScheduleName = "schedule"
	RemoteIOName  = "remote-io"
	CSWWriteName = "csw-write"
	RequestReadName = "request-read"
)

type NumericalWare interface {
	Add(key string, name string, data int64)
	// reset num
	Reset()

	Average() int64
	Sum() int64
	OutPut() []*MultiLevelSt

	OutputWithBaseLine(baseLine int64) *NumericalResult
}

type MultiLevelSt struct {
	ma map[string]int64
}

type numericalWare struct {
	sync.Mutex
	dataMap map[string]*MultiLevelSt
	sum   int64
	count int64
	cap   int64
}

func NewNumericalWare() NumericalWare {
	return &numericalWare{
		dataMap: make(map[string]*MultiLevelSt, 10000),
		cap: 10000,
	}
}

func (n *numericalWare) Add(key string, name string, data int64) {
	go n.add(key, name, data)
}

func (n *numericalWare) add(key string, name string, data int64) {
	n.Lock()
	defer n.Unlock()

	var d *MultiLevelSt

	d, exist := n.dataMap[key]
	if !exist {
		d = &MultiLevelSt{
			ma: make(map[string]int64),
		}
		n.dataMap[key] = d
		n.count ++
	}

	d.ma[name] = data

	if name == TotalName {
		n.sum += data
	}
}

func (n *numericalWare)  Reset() {
	n.Lock()
	defer n.Unlock()

	n.sum = 0
	n.dataMap = make(map[string]*MultiLevelSt, 10000)
	n.count = 0
}

func (n *numericalWare)  Average() int64 {
	n.Lock()
	defer n.Unlock()

	if n.count == 0 {
		return 0
	}
	return n.sum / n.count
}

func (n *numericalWare)  Sum() int64 {
	n.Lock()
	defer n.Unlock()

	return n.sum
}

func (n *numericalWare) OutPut() []*MultiLevelSt {
	n.Lock()
	defer n.Unlock()

	copyData := make([]*MultiLevelSt, n.count)
	i := 0
	for _, v := range n.dataMap {
		copyData[i] = v
		i ++
	}

	return copyData
}

func (n *numericalWare) OutputWithBaseLine(baseLine int64) *NumericalResult {
	var(
		totalCount = 0
		ioCount = 0
		scheduleCount = 0
		cswWriteCount = 0
		requestReadCount = 0

		totalSum int64 = 0
		ioSum int64 = 0
		scheduleSum int64 = 0
		cswWriteSum int64 = 0
		requestReadSum int64 = 0
	)

	data := n.OutPut()
	if len(data) == 0 {
		return &NumericalResult{}
	}

	resultData := make([][]int64, len(data))

	for i, d := range data {
		singleD := make([]int64, 5)
		total := mapValue(d.ma, TotalName)
		if total > 0 {
			totalCount ++
			singleD[0] = total
			totalSum += total
		}

		rio := mapValue(d.ma, RemoteIOName)
		if rio > 0 {
			ioCount ++
			singleD[1] = rio
			ioSum += rio
		}

		sh := mapValue(d.ma, ScheduleName)
		if sh > 0 {
			scheduleCount ++
			singleD[2] = sh
			scheduleSum += sh
		}

		cw := mapValue(d.ma, CSWWriteName)
		if cw > 0 {
			cswWriteCount ++
			singleD[3] = cw
			cswWriteSum += cw
		}

		rr := mapValue(d.ma, RequestReadName)
		if rr > 0 {
			requestReadCount ++
			singleD[4] = rr
			requestReadSum += rr
		}

		resultData[i] = singleD
	}

	ioAve := ioSum / int64(ioCount)
	scheduleAve := scheduleSum / int64(scheduleCount)
	cswWriteAve := cswWriteSum / int64(cswWriteCount)
	requestReadAve := requestReadSum / int64(requestReadCount)

	rs := &NumericalResult{
		SumIO: ioSum,
		AverageIO: ioAve,
		SumSchedule: scheduleSum,
		AverageSchedule: scheduleAve,
		SumCwsWrite: cswWriteSum,
		AverageCwsWrite: cswWriteAve,
		SumRequestRead: requestReadSum,
		AverageRequestRead: requestReadAve,
		Data: resultData,
		BaseLine: baseLine,
	}

	rs.autoCompute()
	return rs
}

type  numericalReader struct {
	io.Reader
	start time.Time
	n NumericalWare
	key  string
}

func NewNumericalReader(rd io.Reader, startTime time.Time, key string, n NumericalWare) *numericalReader {
	return &numericalReader{
		Reader: rd,
		start: startTime,
		n: n,
		key: key,
	}
}

func (r *numericalReader) Close() error {
	logrus.Infof("close the reader")
	cost := time.Now().Sub(r.start).Nanoseconds()
	r.n.Add(r.key, TotalName, cost)
	return nil
}

type NumericalResult struct {
	Sum 	int64
	Average int64
	SumIO   int64
	AverageIO int64
	SumSchedule int64
	AverageSchedule int64
	SumCwsWrite int64
	AverageCwsWrite int64
	SumRequestRead int64
	AverageRequestRead int64

	Data	[][]int64

	BaseLine	int64
	// the sum sub the base data
	BaseLineSum 	int64

	BaseLineAverage int64


	Range1ms 	 int64
	Range1To10ms int64
	Range10To50ms int64
	Range50To100ms int64
	Range100To500ms int64
	Range500To1000ms int64
	RangeOut1s		int64

	Err				error
}

func (r *NumericalResult) autoCompute() {
	var sum int64 = 0
	count := len(r.Data)
	for _, d := range r.Data {
		r.filter(d[0])
		sum += d[0]
	}

	if r.Sum == 0 {
		r.Sum = sum
	}

	if r.Average == 0 {
		if count != 0 {
			r.Average = r.Sum / int64(count)
		}
	}

	r.BaseLineSum = r.Sum - r.BaseLine * int64(count)
	r.BaseLineAverage = r.Average - r.BaseLine
}

func (r *NumericalResult) filter(data int64) {
	const mNumber = 1000 * 1000
	dataWithBaseLine := data - r.BaseLine

	if dataWithBaseLine < mNumber {
		r.Range1ms ++
		return
	}

	if dataWithBaseLine < 10 * mNumber {
		r.Range1To10ms ++
		return
	}

	if dataWithBaseLine < 50 * mNumber {
		r.Range10To50ms ++
		return
	}

	if dataWithBaseLine < 100 * mNumber {
		r.Range50To100ms ++
		return
	}

	if dataWithBaseLine < 500 * mNumber {
		r.Range100To500ms ++
		return
	}

	if dataWithBaseLine < 1000 * mNumber {
		r.Range500To1000ms ++
		return
	}

	r.RangeOut1s ++
}

func mapValue(m map[string]int64, key string) int64 {
	v, ok := m[key]
	if ok {
		return v
	}

	return -1
}
