package task

import (
	"context"

	"github.com/hjwalt/flows/message"
)

type Producer interface {
	Produce(context.Context, Task[message.Bytes]) error
	Start() error
	Stop() error
}
