package transport

import (
	"io"
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
}

type numericalWare struct {
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
	n.data = append(n.data, data)
	n.sum += data
	n.count = n.count + 1
}

func (n *numericalWare)  Reset() {
	n.sum = 0
	n.data = make([]int64, 10000)
	n.count = 0
}

func (n *numericalWare)  Average() int64 {
	if n.count == 0 {
		return 0
	}
	return n.sum / n.count
}

func (n *numericalWare)  Sum() int64 {
	return n.sum
}

func (n *numericalWare) OutPut() []int64 {
	copyData := make([]int64, n.count)
	copy(copyData, n.data[:n.count])
	return copyData
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
	return nil
}

type NumericalResult struct {
	Sum 	int64
	Average int64
	Data	[]int64
}
