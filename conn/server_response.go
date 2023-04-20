package conn

import (
	"context"
	"log"
	"runtime/debug"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/response"
)

// GetResponseStream Read Response(s) until EOS or Exception
func (g *GatewayConn) GetResponseStream(ctx context.Context) <-chan response.Packet {
	responseChannel := make(chan response.Packet, 10)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		defer close(responseChannel)
		defer func() {
			g.inQuery = false
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			resp, err := response.ReadPacketWithLocation(g.decoder, g.compress, data.ClickHouseRevision, g.serverInfo.Timezone)
			if err != nil {
				responseChannel <- &response.ExceptionPacket{
					Message: err.Error(),
				}
				return
			}
			responseChannel <- resp
			switch resp.(type) {
			case *response.ExceptionPacket, *response.EndOfStreamPacket:
				return
			}
		}
	}()

	return responseChannel
}

// SendQueryAssertNoError sends query to server, flushes all response from
// server, returning error if any.
func (g *GatewayConn) SendQueryAssertNoError(ctx context.Context, query string) error {
	if err := g.SendQuery(query); err != nil {
		return err
	}
	return g.FlushServerResponses(ctx)
}

// FlushServerResponses discards all server response, returning exception if any.
func (g *GatewayConn) FlushServerResponses(ctx context.Context) error {
	for res := range g.GetResponseStream(ctx) {
		if res, ok := res.(*response.ExceptionPacket); ok {
			return res
		}
	}
	return nil
}
