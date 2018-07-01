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
		_, err := q.queue.EnqueueString(data)
		return err
	}
	return fmt.Errorf("queue is nil")
}

func (q *Queue) dequeue(queue *ds.Queue, timeout int64) (string, string, error) {
	item, err := queue.Dequeue()
	if err != nil {
		return "", "", err
	}
	key := ""
	if timeout > 0 {
		now := time.Now().Unix()
		key = goutil.TimeStr(now+timeout) + ":" + goutil.ContentMD5(item.Value)
		if err = q.addToRunning(key, item.Value); err != nil {
			return "", "", err
		}
	}
	return key, string(item.Value), nil
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

func (q *Queue) addToRunning(key string, value []byte) error {
	if len(value) == 0 {
		return fmt.Errorf("empty value")
	}
	if q.runningStore == nil {
		return fmt.Errorf("runningStore is nil")
	}
	return q.runningStore.Put(key, value)
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
					if _, err := q.retryQueue.Enqueue(value); err != nil {
						return false, err
					}
					q.runningStore.Delete(string(key))
					return true, nil
				})
			goutil.Sleep(5*time.Second, q.exit)
		}
	}
}
