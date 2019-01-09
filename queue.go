package q

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/liuzl/ds"
	"github.com/liuzl/goutil"
	"github.com/liuzl/store"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Queue struct {
	Path string `json:"path"`

	queue        *ds.Queue
	retryQueue   *ds.Queue
	runningStore *store.LevelStore
	exit         chan bool
	retryLimit   int
}

type valueCnt struct {
	Value []byte
	Cnt   int
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
	if q.queue, err = ds.OpenQueue(queueDir); err != nil {
		return nil, err
	}
	retryDir := filepath.Join(path, "retry_queue")
	if q.retryQueue, err = ds.OpenQueue(retryDir); err != nil {
		return nil, err
	}
	storeDir := filepath.Join(path, "running")
	if q.runningStore, err = store.NewLevelStore(storeDir); err != nil {
		return nil, err
	}

	go q.retry()

	return q, nil
}

func (q *Queue) Status() map[string]interface{} {
	if q.queue == nil {
		return map[string]interface{}{"error": "queue is nil"}
	}
	return map[string]interface{}{
		"queue_length":       q.queue.Length(),
		"retry_queue_length": q.retryQueue.Length(),
	}
}

func (q *Queue) Enqueue(data string) error {
	if q.queue != nil {
		_, err := q.queue.EnqueueObject(&valueCnt{Value: []byte(data)})
		return err
	}
	return fmt.Errorf("queue is nil")
}

func (q *Queue) peek(queue *ds.Queue) (string, error) {
	item, err := queue.Peek()
	if err != nil {
		return "", err
	}
	var v valueCnt
	if err = store.BytesToObject(item.Value, &v); err != nil {
		return "", err
	}
	return string(v.Value), nil
}

func (q *Queue) Peek() (string, error) {
	if q.retryQueue != nil && q.retryQueue.Length() > 0 {
		return q.peek(q.retryQueue)
	}
	if q.queue != nil && q.queue.Length() > 0 {
		return q.peek(q.queue)
	}
	return "", fmt.Errorf("Queue is empty")
}

func (q *Queue) dequeue(queue *ds.Queue, timeout int64) (string, string, error) {
	item, err := queue.Dequeue()
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
		if err = q.addToRunning(key, v.Value, v.Cnt+1); err != nil {
			return "", "", err
		}
	}
	return key, string(v.Value), nil
}

func (q *Queue) Dequeue(timeout int64) (string, string, error) {
	if q.retryQueue != nil && q.retryQueue.Length() > 0 {
		return q.dequeue(q.retryQueue, timeout)
	}
	if q.queue != nil && q.queue.Length() > 0 {
		return q.dequeue(q.queue, timeout)
	}
	return "", "", fmt.Errorf("Queue is empty")
}

func (q *Queue) Confirm(key string) error {
	if q.runningStore == nil {
		return fmt.Errorf("runningStore is nil")
	}
	return q.runningStore.Delete(key)
}

func (q *Queue) Close() {
	if q.exit != nil {
		q.exit <- true
	}
	if q.queue != nil {
		q.queue.Close()
	}
	if q.retryQueue != nil {
		q.retryQueue.Close()
	}
	if q.runningStore != nil {
		q.runningStore.Close()
	}
}

func (q *Queue) Drop() {
	q.Close()
	os.RemoveAll(q.Path)
}

func (q *Queue) addToRunning(key string, value []byte, cnt int) error {
	if len(value) == 0 {
		return fmt.Errorf("empty value")
	}
	if q.runningStore == nil {
		return fmt.Errorf("runningStore is nil")
	}
	v, err := store.ObjectToBytes(valueCnt{value, cnt})
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
			now := time.Now().Format("20060102030405")
			q.runningStore.ForEach(&util.Range{Limit: []byte(now)},
				func(key, value []byte) (bool, error) {
					var v valueCnt
					if err := store.BytesToObject(value, &v); err != nil {
						return false, err
					}
					if _, err := q.retryQueue.EnqueueObject(v); err != nil {
						return false, err
					}
					q.runningStore.Delete(string(key))
					return true, nil
				})
			goutil.Sleep(5*time.Second, q.exit)
		}
	}
}
