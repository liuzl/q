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

func EnqueueHandler(w http.ResponseWriter, r *http.Request) {
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
	rest.MustEncode(w, rest.RestMessage{Status: "ok", Message: nil})
}

func DequeueHandler(w http.ResponseWriter, r *http.Request) {
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
		rest.ErrInternalServer(w, err)
		return
	}
	rest.MustEncode(w, rest.RestMessage{Status: "ok", Message: map[string]string{
		"key": key, "value": value,
	}})
}

func ConfirmHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s  method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	r.ParseForm()
	key := strings.TrimSpace(r.FormValue("key"))
	if key == "" {
		rest.ErrBadRequest(w, "empty key")
		return
	}
	if err := queue.Confirm(key); err != nil {
		rest.ErrInternalServer(w, err)
		return
	}
	rest.MustEncode(w, rest.RestMessage{Status: "ok", Message: nil})
}

func StatusHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s  method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	rest.MustEncode(w, queue.Status())
}

func PeekHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s  method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	r.ParseForm()
	value, err := queue.Peek()
	if err != nil {
		rest.ErrInternalServer(w, err.Error())
		return
	}
	rest.MustEncode(w, rest.RestMessage{Status: "ok", Message: map[string]string{
		"value": value,
	}})
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

	fuego.GetStd(s, "/enqueue/", EnqueueHandler).Param("query", "data", "Data to enqueue", fuego.OpenAPIParam{Required: true, Type: "string"})
	fuego.GetStd(s, "/dequeue/", DequeueHandler).Param("query", "timeout", "Timeout in seconds", fuego.OpenAPIParam{Type: "int", Example: "300"})
	fuego.GetStd(s, "/confirm/", ConfirmHandler).Param("query", "key", "Key to confirm", fuego.OpenAPIParam{Required: true, Type: "string"})
	fuego.GetStd(s, "/status/", StatusHandler)
	fuego.GetStd(s, "/peek/", PeekHandler)

	glog.Info("qserver listen on", *addr)
	s.Run()
}
