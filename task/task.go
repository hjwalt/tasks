package task

import "time"

type Task[V any] struct {
	Value     V
	Headers   map[string]any
	Timestamp time.Time
}
