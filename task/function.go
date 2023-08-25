package task

import (
	"context"

	"github.com/hjwalt/flows/message"
)

type HandlerFunction func(context.Context, Task[message.Bytes]) ([]message.Message[message.Bytes, message.Bytes], error)
