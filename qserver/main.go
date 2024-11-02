package main

import (
	"encoding/base64"
	"flag"
	"net/http"
	"strconv"
	"strings"

	"github.com/golang/glog"
	"github.com/liuzl/q"
	"zliu.org/goutil/rest"
)

var (
	addr  = flag.String("addr", ":9080", "bind address")
	path  = flag.String("path", "./queue", "task queue dir")
	queue *q.Queue
)

func EnqueueHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s  method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	r.ParseForm()
	data := strings.TrimSpace(r.FormValue("data"))
	if data == "" {
		rest.MustEncode(w, rest.RestMessage{Status: "error", Message: "data is empty"})
		return
	}
	if err := queue.Enqueue(data); err != nil {
		rest.MustEncode(w, rest.RestMessage{Status: "error", Message: err.Error()})
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
		timeout = 300
	}
	key, value, err := queue.Dequeue(timeout)
	if err != nil {
		rest.MustEncode(w, rest.RestMessage{Status: "error", Message: err.Error()})
		return
	}
	rest.MustEncode(w, rest.RestMessage{Status: "ok", Message: map[string]string{
		"key": key, "value": base64.StdEncoding.EncodeToString([]byte(value)),
	}})
}

func ConfirmHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s  method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	r.ParseForm()
	key := strings.TrimSpace(r.FormValue("key"))
	if key == "" {
		rest.MustEncode(w, rest.RestMessage{Status: "error", Message: "empty key"})
		return
	}
	if err := queue.Confirm(key); err != nil {
		rest.MustEncode(w, rest.RestMessage{Status: "error", Message: err.Error()})
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
		rest.MustEncode(w, rest.RestMessage{Status: "error", Message: err.Error()})
		return
	}
	rest.MustEncode(w, rest.RestMessage{Status: "ok", Message: map[string]string{
		"value": base64.StdEncoding.EncodeToString([]byte(value)),
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
	http.Handle("/dequeue/", rest.WithLog(DequeueHandler))
	http.Handle("/peek/", rest.WithLog(PeekHandler))
	http.Handle("/enqueue/", rest.WithLog(EnqueueHandler))
	http.Handle("/confirm/", rest.WithLog(ConfirmHandler))
	http.Handle("/status/", rest.WithLog(StatusHandler))
	glog.Info("qserver listen on", *addr)
	glog.Error(http.ListenAndServe(*addr, nil))
}
