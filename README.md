# Q

[![GoDoc](https://godoc.org/github.com/liuzl/q?status.svg)](https://godoc.org/github.com/liuzl/q)

Q is a simple persistent message/task queue. It is written in Golang.

It has the following features:

- [Message confirmation and retry](#confirm-retry)
- [Message persistent](#persistent)

Messages in Q are very safe. They exist until the consumer confirms clearly, otherwise messages would be recycled and added back to queue again.
