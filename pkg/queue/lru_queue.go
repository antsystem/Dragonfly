package queue

import (
	"container/list"
	"sync"

	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
)

type cQElementData struct {
	key  string
	data interface{}
}

type LRUQueue struct {
	sync.Mutex
	capacity int

	itemMap map[string]*list.Element
	l       *list.List
}

func NewLRUQueue(capacity int) *LRUQueue {
	return &LRUQueue{
		capacity: capacity,
		itemMap:  make(map[string]*list.Element, capacity),
		l:        list.New(),
	}
}

// put item to front, return the obsolete item
func (q *LRUQueue) Put(key string, data interface{})  (obsoleteKey string, obsoleteData interface{}) {
	q.Lock()
	defer q.Unlock()

	if i, ok := q.itemMap[key]; ok {
		//todo: update data
		i.Value.(*cQElementData).data = data
		q.internalPutFront(i)
		return
	}

	if len(q.itemMap) >= q.capacity {
		// remove the earliest item
		i := q.internalRemoveTail()
		if i != nil {
			delete(q.itemMap, i.Value.(*cQElementData).key)
			obsoleteKey = key
			obsoleteData = i.Value.(*cQElementData).data
		}
	}

	i := q.internalPutValue(&cQElementData{key: key, data: data})
	q.itemMap[key] = i
	return
}

// getFront will get several item from front and not poll out them.
func (q *LRUQueue) GetFront(count int) []interface{} {
	q.Lock()
	defer q.Unlock()

	result := make([]interface{}, count)
	item := q.l.Front()
	index := 0
	for {
		if item == nil {
			break
		}

		result[index] = item.Value.(*cQElementData).data
		index++
		if index >= count {
			break
		}

		item = item.Next()
	}

	return result[:index]
}

func (q *LRUQueue) GetItemByKey(key string) (interface{}, error) {
	q.Lock()
	defer q.Unlock()

	if data, exist := q.itemMap[key]; exist {
		return data.Value.(*cQElementData).data, nil
	}

	return nil, errortypes.ErrDataNotFound
}

func (q *LRUQueue) Delete() {

}

func (q *LRUQueue) internalPutFront(i *list.Element) {
	q.l.MoveToFront(i)
}

func (q *LRUQueue) internalPutValue(data interface{}) *list.Element {
	e := q.l.PushFront(data)
	return e
}

func (q *LRUQueue) internalRemoveTail() *list.Element {
	e := q.l.Back()
	q.l.Remove(e)

	return e
}
