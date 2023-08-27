package task

import "time"

type Message[V any] struct {
	Value     V
	Headers   map[string]any
	Timestamp time.Time
}
