package q

import (
	"net/http"
	"strconv"
	"strings"

	"zliu.org/goutil/rest"
)

var zlog = rest.Log()

type QServer struct {
	queue *Queue
}

func NewQServer(path string) (*QServer, error) {
	queue, err := NewQueue(path)
	if err != nil {
		return nil, err
	}
	return &QServer{queue: queue}, nil
}

func (qs *QServer) Close() {
	qs.queue.Close()
}

// 创建任务
func (qs *QServer) CreateTaskHandler(w http.ResponseWriter, r *http.Request) {
	zlog.Info().Str("addr", r.RemoteAddr).Str("method", r.Method).Str("host", r.Host).Str("uri", r.RequestURI).Msg("create task")
	r.ParseForm()
	data := strings.TrimSpace(r.FormValue("data"))
	if data == "" {
		rest.ErrBadRequest(w, "data is empty")
		return
	}
	if err := qs.queue.Enqueue(data); err != nil {
		rest.ErrInternalServer(w, err)
		return
	}
	rest.MustEncode(w, map[string]string{"status": "ok"})
}

// 获取任务
func (qs *QServer) GetTaskHandler(w http.ResponseWriter, r *http.Request) {
	zlog.Info().Str("addr", r.RemoteAddr).Str("method", r.Method).Str("host", r.Host).Str("uri", r.RequestURI).Msg("get task")
	r.ParseForm()
	t := strings.TrimSpace(r.FormValue("timeout"))
	timeout, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		rest.ErrBadRequest(w, "invalid timeout")
		return
	}
	key, value, err := qs.queue.Dequeue(timeout)
	if err != nil {
		if err.Error() == "Queue is empty" {
			rest.ErrorMessageWithStatus(w, err.Error(), http.StatusNotFound)
		} else {
			rest.ErrInternalServer(w, err.Error())
		}
		return
	}
	rest.MustEncode(w, map[string]string{"key": key, "value": value})
}

// 更新任务状态
func (qs *QServer) UpdateTaskStatusHandler(w http.ResponseWriter, r *http.Request) {
	zlog.Info().Str("addr", r.RemoteAddr).Str("method", r.Method).Str("host", r.Host).Str("uri", r.RequestURI).Msg("update task status")

	key := r.PathValue("key")
	if key == "" {
		rest.ErrBadRequest(w, "empty key")
		return
	}
	if err := qs.queue.Confirm(key); err != nil {
		if strings.HasPrefix(err.Error(), "key not found") {
			rest.ErrorMessageWithStatus(w, err.Error(), http.StatusNotFound)
		} else {
			rest.ErrInternalServer(w, err)
		}
		return
	}
	rest.MustEncode(w, map[string]string{"status": "ok"})
}

// 获取队列状态
func (qs *QServer) GetQueueStatusHandler(w http.ResponseWriter, r *http.Request) {
	zlog.Info().Str("addr", r.RemoteAddr).Str("method", r.Method).Str("host", r.Host).Str("uri", r.RequestURI).Msg("get queue status")
	rest.MustEncode(w, qs.queue.Status())
}

// 获取下一个任务预览
func (qs *QServer) GetNextTaskHandler(w http.ResponseWriter, r *http.Request) {
	zlog.Info().Str("addr", r.RemoteAddr).Str("method", r.Method).Str("host", r.Host).Str("uri", r.RequestURI).Msg("get next task")
	r.ParseForm()
	value, err := qs.queue.Peek()
	if err != nil {
		if err.Error() == "Queue is empty" {
			rest.ErrorMessageWithStatus(w, err.Error(), http.StatusNotFound)
		} else {
			rest.ErrInternalServer(w, err.Error())
		}
		return
	}
	rest.MustEncode(w, map[string]string{"value": value})
}
