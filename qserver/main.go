package main

import (
	"flag"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-fuego/fuego"
	"github.com/golang/glog"
	"github.com/liuzl/q"
	"zliu.org/goutil/rest"
)

var (
	addr  = flag.String("addr", "127.0.0.1:9080", "bind address")
	path  = flag.String("path", "./queue", "task queue dir")
	queue *q.Queue
)

// 创建任务
func CreateTaskHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s  method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	r.ParseForm()
	data := strings.TrimSpace(r.FormValue("data"))
	if data == "" {
		rest.ErrBadRequest(w, "data is empty")
		return
	}
	if err := queue.Enqueue(data); err != nil {
		rest.ErrInternalServer(w, err)
		return
	}
	rest.MustEncode(w, map[string]string{"status": "ok"})
}

// 获取任务
func GetTaskHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s  method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	r.ParseForm()
	t := strings.TrimSpace(r.FormValue("timeout"))
	timeout, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		rest.ErrBadRequest(w, "invalid timeout")
		return
	}
	key, value, err := queue.Dequeue(timeout)
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
func UpdateTaskStatusHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s  method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)

	key := r.PathValue("key")
	if key == "" {
		rest.ErrBadRequest(w, "empty key")
		return
	}
	if err := queue.Confirm(key); err != nil {
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
func GetQueueStatusHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s  method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	rest.MustEncode(w, queue.Status())
}

// 获取下一个任务预览
func GetNextTaskHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s  method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	r.ParseForm()
	value, err := queue.Peek()
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

func main() {
	flag.Parse()
	defer glog.Flush()
	var err error
	if queue, err = q.NewQueue(*path); err != nil {
		glog.Fatal(err)
	}
	defer queue.Close()
	defer glog.Info("server exit")

	s := fuego.NewServer(fuego.WithAddr(*addr))

	fuego.PostStd(s, "/tasks", CreateTaskHandler).
		Param("query", "data", "Data to enqueue", fuego.OpenAPIParam{Required: true, Type: "string"})

	fuego.GetStd(s, "/tasks", GetTaskHandler).
		Param("query", "timeout", "Timeout in seconds", fuego.OpenAPIParam{Type: "int", Example: "300"})

	fuego.PutStd(s, "/tasks/{key}", UpdateTaskStatusHandler)

	fuego.GetStd(s, "/tasks/status", GetQueueStatusHandler)

	fuego.GetStd(s, "/tasks/next", GetNextTaskHandler)

	glog.Info("qserver listen on", *addr)
	s.Run()
}
