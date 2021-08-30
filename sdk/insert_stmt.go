package sdk

import (
	"context"
	"fmt"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/response"
	"github.com/bytehouse-cloud/driver-go/errors"
	"github.com/bytehouse-cloud/driver-go/stream"
)

type ColumnValuesPool interface {
	Get() [][]interface{}
	Put([][]interface{})
}

type InsertStmt struct {
	// Column values to be converted into blocks
	columnsBuffer      [][]interface{}
	bufferPool         ColumnValuesPool
	columnsInputStream chan [][]interface{}
	insertProcess      *stream.InsertProcess
	toBlockProcess     stream.AsyncToBlockProcess
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
	parallelism := resolveInsertBlockParallelism(ctx)
	newStmt := &InsertStmt{
		bufferPool:         stream.NewColumnValuesPool(sample.NumColumns, sample.NumRows),
		insertProcess:      insertProcess,
		columnsInputStream: columnsInputStream,
	}
	newStmt.toBlockProcess = stream.NewAsyncColumnValuesToBlock(columnsInputStream, sample,
		stream.OptionSetParallelism(parallelism),
		stream.OptionSetRecycle(newStmt.bufferPool.Put),
	)
	newStmt.columnsBuffer = newStmt.getBuffer()

	blockInputStream := newStmt.toBlockProcess.Start(ctx)
	insertProcess.Start(ctx, blockInputStream, serverResponseStream)

	return newStmt
}

func (s *InsertStmt) getBuffer() [][]interface{} {
	colBuf := s.bufferPool.Get()
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
		if err := s.toBlockProcess.Error(); err != nil {
			return err
		}
		if err := s.insertProcess.Error(); err != nil {
			return err
		}
	}

	if len(args) != s.insertProcess.NumColumns() {
		return errors.ErrorfWithCaller("invalid args len, expected = %d, got = %d", s.insertProcess.NumColumns(), len(args))
	}

	for i := range s.columnsBuffer {
		s.columnsBuffer[i] = append(s.columnsBuffer[i], args[i])
	}

	// only flush values when size of columns buffer is the same as batch
	if len(s.columnsBuffer[0]) < s.insertProcess.BatchSize() {
		return nil
	}

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
