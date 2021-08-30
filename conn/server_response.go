package conn

import (
	"context"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/response"
)

// GetResponseStream Read Response(s) until EOS or Exception
func (g *GatewayConn) GetResponseStream(ctx context.Context) <-chan response.Packet {
	responseChannel := make(chan response.Packet, 10)

	go func() {
		defer close(responseChannel)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			resp, err := response.ReadPacket(g.decoder, g.compress, data.ClickHouseRevision)
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
