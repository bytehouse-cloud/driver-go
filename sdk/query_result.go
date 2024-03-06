package sdk

import (
	"io"
	"log"
	"runtime/debug"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/driver/response"
)

type QueryResult struct {
	dataStream   chan *response.DataPacket //todo: change to chan *data.Block
	block        *data.Block
	values       [][]interface{}
	offset       int
	columns      []*column.CHColumn // columns are to be used only for metadata consumption
	err          error
	resultMeta   []response.Packet
	rowsInserted int
}

func NewInsertQueryResult(responses <-chan response.Packet) *QueryResult {
	qr := &QueryResult{
		dataStream: make(chan *response.DataPacket, 0),
		resultMeta: make([]response.Packet, 0),
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		defer close(qr.dataStream)

		for resp := range responses {
			switch resp := resp.(type) {
			case *response.ExceptionPacket:
				qr.err = resp
			default:
				qr.resultMeta = append(qr.resultMeta, resp)
			}
		}
	}()

	return qr
}

func NewQueryResult(responses <-chan response.Packet, finish func()) *QueryResult {
	qr := &QueryResult{
		dataStream: make(chan *response.DataPacket, 25),
		resultMeta: make([]response.Packet, 0),
	}

	waitReady(responses, qr)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		defer finish()
		defer close(qr.dataStream)

		for resp := range responses {
			switch resp := resp.(type) {
			case *response.DataPacket:
				b := resp.Block
				if b.NumRows == 0 || b.NumColumns == 0 {
					continue
				}
				qr.dataStream <- resp
			case *response.ExceptionPacket:
				qr.err = resp
			default:
				qr.resultMeta = append(qr.resultMeta, resp)
			}
		}
	}()

	return qr
}

// waitReady blocks until it receives critical components of responses, which will make it safe for caller usage.
// assigns all responses to given QueryResult
func waitReady(responses <-chan response.Packet, qr *QueryResult) {
	for resp := range responses {
		switch resp := resp.(type) {
		case *response.DataPacket:
			b := resp.Block
			if len(b.Columns) == 0 {
				continue
			}
			// When send "insert into ... select ... settings enable_optimizer = 1" sql, waitReady will return early.
			// So add conditional judgment to make it block
			if len(b.Columns) > 0 && b.NumRows == 0 {
				qr.columns = b.Columns
				continue
			}
			if b.NumRows > 0 {
				qr.dataStream <- resp
			}
			return
		case *response.ExceptionPacket:
			qr.err = resp
			return
		case *response.EndOfStreamPacket:
			return
		default:
			qr.resultMeta = append(qr.resultMeta, resp)
		}
	}
}

func (q *QueryResult) Columns() []*column.CHColumn {
	if q.columns == nil {
		d := <-q.dataStream
		if d == nil {
			return nil
		}
		q.readBlockData(d.Block)
	}
	return q.columns
}

func (q *QueryResult) GetAllMeta() []response.Packet {
	return q.resultMeta
}

func (q *QueryResult) GetAllLogs() []*response.LogPacket {
	logs := make([]*response.LogPacket, 0)
	for i := range q.resultMeta {
		if log, ok := q.resultMeta[i].(*response.LogPacket); ok {
			logs = append(logs, log)
		}
	}
	return logs
}

func (q *QueryResult) Exception() error {
	return q.err
}

func (q *QueryResult) NextRow() ([]interface{}, bool) {
	if len(q.values) == q.offset {
		d := <-q.dataStream
		if d == nil {
			return nil, false
		}
		q.readBlockData(d.Block)
	}
	return q.getNextRowFromBuffer(), true
}

func (q *QueryResult) NextRowAsString() ([]interface{}, bool) {
	if len(q.values) == q.offset {
		d := <-q.dataStream
		if d == nil {
			return nil, false
		}
		q.readBlockDataInStrings(d.Block)
	}
	return q.getNextRowFromBuffer(), true
}

func (q *QueryResult) getNextRowFromBuffer() []interface{} {
	row := q.values[q.offset]
	q.offset++
	return row
}

func (q *QueryResult) readBlockData(block *data.Block) {
	q.prepareValues(block)
	block.WriteToValues(q.values)
	q.offset = 0
	if q.block == nil {
		q.block = block
		return
	}
	_ = q.block.Close()
	q.block = block
}

func (q *QueryResult) readBlockDataInStrings(block *data.Block) {
	q.prepareValues(block)
	block.WriteValuesAsString(q.values)
	q.offset = 0
	if q.block == nil {
		q.block = block
		return
	}
	_ = q.block.Close()
	q.block = block
}

func (q *QueryResult) prepareValues(block *data.Block) {
	if len(q.values) < block.NumRows {
		if cap(q.values) < block.NumRows {
			q.values = make([][]interface{}, block.NumRows)
		}
		q.values = q.values[:block.NumRows]
	}
	q.values = q.values[:block.NumRows]
	if len(q.values[0]) < block.NumColumns {
		for i := range q.values {
			q.values[i] = expand(q.values[i], block.NumColumns)
		}
	}
	for i := range q.values {
		q.values[i] = q.values[i][:block.NumColumns]
	}
}

func (q *QueryResult) ExportToReader(fmtType string) io.Reader {
	return newResultFmtReader(fmtType, extractBlockStream(q.dataStream))
}

func (q *QueryResult) Close() error {
	for d := range q.dataStream {
		_ = d.Close()
	}
	return nil
}

func expand(values []interface{}, numCol int) []interface{} {
	if cap(values) < numCol {
		return make([]interface{}, numCol)
	}
	return values
}

func extractBlockStream(dataRespStream <-chan *response.DataPacket) <-chan *data.Block {
	blockStream := make(chan *data.Block, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		defer close(blockStream)
		for r := range dataRespStream {
			blockStream <- r.Block
		}
	}()

	return blockStream
}
