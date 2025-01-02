package main

import (
	"flag"

	"github.com/go-fuego/fuego"
	"github.com/go-fuego/fuego/option"
	"github.com/go-fuego/fuego/param"
	"github.com/liuzl/q"
	"zliu.org/goutil/rest"
)

var (
	addr   = flag.String("addr", "127.0.0.1:9080", "bind address")
	path   = flag.String("path", "./queue", "task queue dir")
	server *q.QServer
	zlog   = rest.Log()
)

func main() {
	flag.Parse()

	var err error
	if server, err = q.NewQServer(*path); err != nil {
		zlog.Fatal().Err(err).Msg("failed to create qserver")
	}
	defer server.Close()
	defer zlog.Info().Msg("server exit")

	s := fuego.NewServer(fuego.WithAddr(*addr))

	fuego.PostStd(s, "/tasks", server.CreateTaskHandler,
		option.Summary("Create a task"),
		option.Description("Create a task to be processed by the queue"),
		option.OperationID("createTask"),
		option.Tags("tasks"),
		option.Query("data", "Data to enqueue"),
	)

	fuego.GetStd(s, "/tasks", server.GetTaskHandler,
		option.Summary("Get a task"),
		option.Description("Get a task from the queue"),
		option.OperationID("getTask"),
		option.Tags("tasks"),
		option.QueryInt("timeout", "Timeout in seconds", param.Default(300)),
	)

	fuego.PutStd(s, "/tasks/{key}", server.UpdateTaskStatusHandler,
		option.Summary("Confirm a task"),
		option.Description("Confirm a task is done"),
		option.OperationID("confirmTask"),
		option.Tags("tasks"),
		option.Path("key", "Task key"),
	)

	fuego.GetStd(s, "/tasks/status", server.GetQueueStatusHandler,
		option.Summary("Get queue status"),
		option.Description("Get queue status"),
		option.OperationID("getQueueStatus"),
		option.Tags("tasks"),
	)

	fuego.GetStd(s, "/tasks/next", server.GetNextTaskHandler,
		option.Summary("Get next task"),
		option.Description("Get next task"),
		option.OperationID("getNextTask"),
		option.Tags("tasks"),
	)

	zlog.Info().Str("addr", *addr).Msg("qserver listen on")
	s.Run()
}
