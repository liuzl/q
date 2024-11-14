package q

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/liuzl/ds"
	"github.com/liuzl/store"
	"github.com/syndtr/goleveldb/leveldb/util"
	"zliu.org/goutil"
)

type Queue struct {
	Path string `json:"path"`

	queue        *ds.PriorityQueue
	runningStore *store.LevelStore
	exit         chan bool
	retryLimit   int
}

type valueCnt struct {
	Value    []byte
	Priority uint8
	Cnt      int
}

func NewQueueWithRetryLimit(path string, limit int) (*Queue, error) {
	q, err := NewQueue(path)
	if err != nil {
		return nil, err
	}
	q.retryLimit = limit
	return q, nil
}

func NewQueue(path string) (*Queue, error) {
	q := &Queue{Path: path, exit: make(chan bool)}
	var err error
	queueDir := filepath.Join(path, "queue")
	if q.queue, err = ds.OpenPriorityQueue(queueDir); err != nil {
		return nil, err
	}
	storeDir := filepath.Join(path, "running")
	if q.runningStore, err = store.NewLevelStore(storeDir); err != nil {
		return nil, err
	}

	go q.retry()
	return q, nil
}

func (q *Queue) Length() uint64 {
	if q.queue == nil {
		return 0
	}
	return q.queue.Length()
}

func (q *Queue) Status() map[string]interface{} {
	if q.queue == nil {
		return map[string]interface{}{"error": "queue is nil"}
	}

	var runningCount int64
	if q.runningStore != nil {
		q.runningStore.ForEach(nil, func(_, _ []byte) (bool, error) {
			runningCount++
			return true, nil
		})
	}

	return map[string]interface{}{
		"queue_length":  q.queue.Length(),
		"running_count": runningCount,
	}
}

func (q *Queue) EnqueueWithPriority(data string, priority uint8) error {
	if q.queue != nil {
		_, err := q.queue.EnqueueObject(&valueCnt{Value: []byte(data), Priority: priority}, priority)
		return err
	}
	return fmt.Errorf("queue is nil")
}

func (q *Queue) Enqueue(data string) error {
	return q.EnqueueWithPriority(data, 128)
}

func (q *Queue) Peek() (string, error) {
	if q.queue == nil || q.queue.Length() == 0 {
		return "", fmt.Errorf("Queue is empty")
	}

	item, err := q.queue.Peek()
	if err != nil {
		return "", err
	}
	var v valueCnt
	if err = store.BytesToObject(item.Value, &v); err != nil {
		return "", err
	}
	return string(v.Value), nil
}

func (q *Queue) Dequeue(timeout int64) (string, string, error) {
	if q.queue == nil || q.queue.Length() == 0 {
		return "", "", fmt.Errorf("Queue is empty")
	}

	item, err := q.queue.Dequeue()
	if err != nil {
		return "", "", err
	}
	key := ""
	var v valueCnt
	if err = store.BytesToObject(item.Value, &v); err != nil {
		return "", "", err
	}
	if timeout > 0 && (q.retryLimit <= 0 || v.Cnt < q.retryLimit) {
		now := time.Now().Unix()
		key = goutil.TimeStr(now+timeout) + ":" + goutil.ContentMD5(item.Value)
		v.Cnt++
		if err = q.addToRunning(key, &v); err != nil {
			return "", "", err
		}
	}
	return key, string(v.Value), nil
}

func (q *Queue) Confirm(key string) error {
	if q.runningStore == nil {
		return fmt.Errorf("runningStore is nil")
	}
	exists, err := q.runningStore.Has(key)
	if err != nil {
		return err
	}
	if exists {
		return q.runningStore.Delete(key)
	}
	return fmt.Errorf("key not found: %s", key)
}

func (q *Queue) Close() {
	if q.exit != nil {
		q.exit <- true
	}
	if q.queue != nil {
		q.queue.Close()
	}
	if q.runningStore != nil {
		q.runningStore.Close()
	}
}

func (q *Queue) Drop() {
	q.Close()
	os.RemoveAll(q.Path)
}

func (q *Queue) addToRunning(key string, item *valueCnt) error {
	if len(item.Value) == 0 {
		return fmt.Errorf("empty value")
	}
	if q.runningStore == nil {
		return fmt.Errorf("runningStore is nil")
	}
	v, err := store.ObjectToBytes(item)
	if err != nil {
		return err
	}
	return q.runningStore.Put(key, v)
}

func (q *Queue) retry() {
	for {
		select {
		case <-q.exit:
			return
		default:
			now := goutil.TimeStr(time.Now().Unix())
			q.runningStore.ForEach(&util.Range{Limit: []byte(now)},
				func(key, value []byte) (bool, error) {
					var v valueCnt
					if err := store.BytesToObject(value, &v); err != nil {
						return false, err
					}
					v.Priority = 0 // retry with highest priority
					if _, err := q.queue.EnqueueObject(v, v.Priority); err != nil {
						return false, err
					}
					q.runningStore.Delete(string(key))
					return true, nil
				})
			goutil.Sleep(5*time.Second, q.exit)
		}
	}
}

// DequeueWithPreviousRetryCount removes and returns the next item in the queue
// along with the number of times it has been previously retried.
// If a timeout is specified, the item is added to the running queue.
func (q *Queue) DequeueWithPreviousRetryCount(timeout int64) (string, string, int, error) {
	if q.queue == nil || q.queue.Length() == 0 {
		return "", "", 0, fmt.Errorf("Queue is empty")
	}

	item, err := q.queue.Dequeue()
	if err != nil {
		return "", "", 0, err
	}

	key := ""
	var v valueCnt
	if err = store.BytesToObject(item.Value, &v); err != nil {
		return "", "", 0, err
	}

	previousRetryCount := v.Cnt
	if timeout > 0 && (q.retryLimit <= 0 || previousRetryCount < q.retryLimit) {
		now := time.Now().Unix()
		key = goutil.TimeStr(now+timeout) + ":" + goutil.ContentMD5(item.Value)
		v.Cnt++
		if err = q.addToRunning(key, &v); err != nil {
			return "", "", 0, err
		}
	}

	return key, string(v.Value), previousRetryCount, nil
}
