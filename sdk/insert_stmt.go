package sdk

import (
	"context"
	"fmt"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/response"
	"github.com/bytehouse-cloud/driver-go/errors"
	"github.com/bytehouse-cloud/driver-go/stream"
	"github.com/bytehouse-cloud/driver-go/stream/values"
)

type getColumnValues func() [][]interface{}

type InsertStmt struct {
	// Column values to be converted into blocks
	columnsBuffer      [][]interface{}
	getEmpty           getColumnValues //TODO: find way to put back into pool after usage
	columnsInputStream chan [][]interface{}
	insertProcess      *stream.InsertProcess
	toBlockProcess     values.BlockProcess
	closed             bool
}

func NewInsertStatement(
	ctx context.Context, sample *data.Block,
	sendBlock stream.SendBlock, cancelInsert stream.CancelInsert,
	serverResponseStream <-chan response.Packet,
	opts ...stream.InsertOption,
) *InsertStmt {
	insertProcess := stream.NewInsertProcess(sample, sendBlock, cancelInsert, opts...)
	columnsInputStream := make(chan [][]interface{}, 1)
	cvPool := values.NewColumnValuesPool(sample.NumColumns, sample.NumRows)
	newStmt := &InsertStmt{
		getEmpty:           cvPool.Get,
		insertProcess:      insertProcess,
		columnsInputStream: columnsInputStream,
	}
	newStmt.toBlockProcess = values.NewColumnValuesToBlock(columnsInputStream, sample)
	newStmt.columnsBuffer = newStmt.getBuffer()

	blockInputStream := newStmt.toBlockProcess.Start(ctx)
	insertProcess.Start(ctx, blockInputStream, serverResponseStream)

	return newStmt
}

func (s *InsertStmt) getBuffer() [][]interface{} {
	colBuf := s.getEmpty()
	for i := range colBuf {
		colBuf[i] = colBuf[i][:0]
	}
	return colBuf
}

func (s *InsertStmt) ExecContext(ctx context.Context, args ...interface{}) (err error) {
	// check context cancellation
	select {
	case <-ctx.Done():
		return err
	default:
		if err = s.toBlockProcess.Error(); err != nil {
			return err
		}
		if err = s.insertProcess.Error(); err != nil {
			return err
		}
	}

	args_len, num_cols := len(args), len(s.columnsBuffer)
	if args_len%num_cols != 0 {
		return errors.ErrorfWithCaller("number of args: %v must be a multiple of number of columns: %v",
			len(args), len(s.columnsBuffer),
		)
	}

	// put all args to the column buffer
	for len(args) > 0 {
		for i, col := range s.columnsBuffer {
			s.columnsBuffer[i] = append(col, args[i])
		}
		args = args[len(s.columnsBuffer):]
	}

	// only flush values when size of columns buffer is the same as batch
	if len(s.columnsBuffer[0]) < s.insertProcess.BatchSize() {
		return nil
	}

	// flushing
	s.columnsInputStream <- s.columnsBuffer
	s.columnsBuffer = s.getBuffer()

	return nil
}

func (s *InsertStmt) Exec(args ...interface{}) (err error) {
	return s.ExecContext(context.Background(), args...)
}

func (s *InsertStmt) Close() error {
	if s.closed {
		return errors.ErrorfWithCaller("insert statement already closed")
	}
	s.closed = true

	if len(s.columnsBuffer[0]) > 0 {
		s.columnsInputStream <- s.columnsBuffer
	}
	close(s.columnsInputStream)

	rowsRead, err := s.toBlockProcess.Finish()
	if err != nil {
		return err
	}
	rowsSent, err := s.insertProcess.Finish()
	if err != nil {
		return err
	}
	if rowsRead != rowsSent {
		return fmt.Errorf(stream.ShortRowsWriteErrFmt, rowsRead, rowsSent)
	}
	return nil
}
