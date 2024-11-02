# Q

[![GoDoc](https://godoc.org/github.com/liuzl/q?status.svg)](https://godoc.org/github.com/liuzl/q)

Q is a simple yet robust persistent message/task queue implemented in Go.

## Features

- **Message Confirmation & Retry**: Messages remain in queue until explicitly confirmed by consumers
- **Message Persistence**: Reliable storage of messages to prevent data loss
- **Priority Queue**: Support for message prioritization to handle urgent tasks first
- **Simple API**: Easy to integrate and use
- **High Performance**: Optimized for speed and efficiency
- **Zero Dependencies**: Standalone implementation with no external requirements

## Safety Guarantees

Messages in Q are handled with strong safety guarantees:
- Messages persist until explicitly confirmed by consumers
- Unconfirmed messages are automatically recycled back to the queue
- No message loss even in case of crashes or failures
- Priority levels are preserved during message recycling and recovery
