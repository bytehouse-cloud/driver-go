package test

import (
	"fmt"
	"testing"

	"github.com/bytehouse-cloud/driver-go/driver/response"
)

func TestExceptionPrint(t *testing.T) {
	r := response.ExceptionPacket{
		Code:       0,
		Name:       "name1",
		Message:    "message1",
		StackTrace: "stack1\nstack11",
		Nested: &response.ExceptionPacket{
			Code:       1,
			Name:       "name2",
			Message:    "message2",
			StackTrace: "stack21\nstack22",
			Nested: &response.ExceptionPacket{
				Code:       2,
				Name:       "name3",
				Message:    "message3",
				StackTrace: "stack31\nstack32\nstack33",
			},
		},
	}
	fmt.Println(r.Error())
}
