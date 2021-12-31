package stream

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/response"
	"github.com/bytehouse-cloud/driver-go/errors"
	"github.com/bytehouse-cloud/driver-go/stream/format"
)

const ShortRowsWriteErrFmt = "short rows write to server, rows read = %d, rows sent = %d"

func HandleInsertFromFmtStream(
	ctx context.Context, respStream <-chan response.Packet,
	blockReader format.BlockStreamFmtReader,
	sendBlock SendBlock, cancelInsert CancelInsert, handleResp CallBackResp,
	opts ...InsertOption,
) (int, error) {
	sample, err := CallBackUntilFirstBlock(ctx, respStream, handleResp)
	if err != nil {
		return 0, err
	}

	eg, ctx := errgroup.WithContext(ctx)
	insertProcess := NewInsertProcess(sample, sendBlock, cancelInsert, opts...)
	blockInputStream, yield := blockReader.BlockStreamFmtRead(ctx, sample, insertProcess.BatchSize())
	insertProcess.Start(ctx, blockInputStream, respStream)

	var rowsRead, rowsSent int
	var readErr, insertErr error
	eg.Go(func() error {
		rowsRead, readErr = yield()
		return readErr
	})
	eg.Go(func() error {
		rowsSent, insertErr = insertProcess.Finish()
		return insertErr
	})
	if err := eg.Wait(); err != nil {
		return rowsSent, err
	}
	if rowsSent != rowsRead {
		return rowsSent, errors.ErrorfWithCaller(ShortRowsWriteErrFmt, rowsRead, rowsSent)
	}
	return rowsSent, nil
}

func CallBackUntilFirstBlock(ctx context.Context, respStream <-chan response.Packet, callBack func(resp response.Packet)) (*data.Block, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, context.Canceled
		case resp, ok := <-respStream:
			if !ok {
				return nil, errors.ErrorfWithCaller("no block received from server")
			}
			switch resp := resp.(type) {
			case *response.DataPacket:
				return resp.Block, nil
			case *response.ExceptionPacket:
				return nil, resp
			default:
				callBack(resp)
			}
		}
	}
}
