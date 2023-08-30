package task

import (
	"context"

	"github.com/hjwalt/flows/flow"
)

type Executor[T any] func(context.Context, Message[T]) error

type Scheduler[K any, V any, T any] func(context.Context) (Message[T], flow.Message[K, V], error)
