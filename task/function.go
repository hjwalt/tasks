package task

import (
	"context"

	"github.com/hjwalt/flows/flow"
	"github.com/hjwalt/runway/structure"
)

type HandlerFunction func(context.Context, Message[structure.Bytes]) ([]flow.Message[structure.Bytes, structure.Bytes], error)

type FlowFunction[IK any, IV any, OK any, OV any, T any] func(context.Context, flow.Message[IK, IV]) (Message[T], flow.Message[OK, OV], error)
