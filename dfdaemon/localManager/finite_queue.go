package localManager

import (
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"sync"
)

// finiteQueue provides a circle queue with capacity, and it will weed out the earlier item if full
type finiteQueue struct {
	sync.Mutex
	capacity  int
	head 	  *itemNode
	tail      *itemNode

	itemMap   map[string]*itemNode
}

type itemNode struct {
	prv  *itemNode
	next *itemNode
	data  interface{}
	key   string
}

func newFiniteQueue(capacity int) *finiteQueue {
	return &finiteQueue{
		capacity: capacity,
		head: nil,
		tail: nil,
		itemMap: make(map[string]*itemNode, capacity),
	}
}

// put item to front
func (q *finiteQueue) putFront(key string, data interface{}) {
	q.Lock()
	defer q.Unlock()

	if i, ok := q.itemMap[key]; ok {
		//todo: update data
		i.data = data
		q.internalPutFront(i)
		return
	}

	if len(q.itemMap) >= q.capacity {
		// remove the earliest item
		i := q.internalRemoveTail()
		if i != nil {
			delete(q.itemMap, i.key)
		}
	}

	node := &itemNode{
		key: key,
		data: data,
	}

	q.itemMap[key] = node
	q.internalPutFront(node)
}

// getFront will get several item from front and not poll out them.
func (q *finiteQueue) getFront(count int) []interface{} {

}

func (q *finiteQueue) getItemByKey(key string) (interface{}, error) {
	q.Lock()
	defer q.Unlock()

	if data, exist := q.itemMap[key]; exist {
		return data, nil
	}

	return nil, errortypes.ErrDataNotFound
}

func (q *finiteQueue) internalPutFront(i *itemNode) {
	if q.head == i {
		return
	}

	if q.head == nil {
		q.head = i
		q.tail = i
		i.prv = i
		i.next = i
		return
	}

	if q.tail == i {
		q.tail = i.prv
	}

	if i.prv != nil && i.next != nil {
		i.prv.next = i.next
		i.next.prv = i.prv
	}

	q.tail.next = i
	q.head.prv = i
	i.prv = q.tail
	i.next = q.head

	q.head = i
}

func (q *finiteQueue) internalRemoveTail() *itemNode {
	if q.tail == nil {
		return nil
	}

	if q.head == q.tail {
		q.head = nil
		q.tail = nil
		return q.tail
	}

	result := q.tail
	q.tail.prv.next = q.head
	q.head.prv = q.tail.prv
	q.tail = q.tail.prv

	return result
}