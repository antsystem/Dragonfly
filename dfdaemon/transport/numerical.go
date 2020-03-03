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

type NumericalWare interface {
	Add(data int64)
	// reset num
	Reset()

	Average() int64
	Sum() int64
	OutPut() []int64

	OutputWithBaseLine(baseLine int64) *NumericalResult
}

type numericalWare struct {
	sync.Mutex
	data  []int64
	sum   int64
	count int64
}

func NewNumericalWare() NumericalWare {
	return &numericalWare{
		data: make([]int64, 10000),
	}
}

func (n *numericalWare)  Add(data int64) {
	go n.add(data)
}

func (n *numericalWare) add(data int64) {
	n.Lock()
	defer n.Unlock()

	n.data = append(n.data, data)
	n.sum += data
	n.count = n.count + 1
}

func (n *numericalWare)  Reset() {
	n.Lock()
	defer n.Unlock()

	n.sum = 0
	n.data = make([]int64, 10000)
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

func (n *numericalWare) OutPut() []int64 {
	n.Lock()
	defer n.Unlock()

	copyData := make([]int64, n.count)
	copy(copyData, n.data[:n.count])
	return copyData
}

func (n *numericalWare) OutputWithBaseLine(baseLine int64) *NumericalResult {
	data := n.OutPut()
	rs := &NumericalResult{
		Data: data,
		BaseLine: baseLine,
	}

	rs.autoCompute()
	return rs
}

type  numericalReader struct {
	io.Reader
	start time.Time
	n NumericalWare
}

func NewNumericalReader(rd io.Reader, startTime time.Time, n NumericalWare) *numericalReader {
	return &numericalReader{
		Reader: rd,
		start: startTime,
		n: n,
	}
}

func (r *numericalReader) Close() error {
	logrus.Infof("close the reader")
	cost := time.Now().Sub(r.start).Nanoseconds()
	r.n.Add(cost)
	logrus.Infof("NumericalWare: %v", r.n)
	return nil
}

type NumericalResult struct {
	Sum 	int64
	Average int64
	Data	[]int64

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
		r.filter(d)
		sum += d
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
