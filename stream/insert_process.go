package stream

import (
	"context"
	"time"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/response"
	"github.com/bytehouse-cloud/driver-go/utils"
)

type (
	SendBlock    func(b *data.Block) error
	CancelInsert func()
	Logf         func(s string, args ...interface{})
	CallBackResp func(resp response.Packet)
)

type InsertProcess struct {
	// Sample block to copy from when construct blocks
	sample *data.Block
	// maximum number of row column buffer can hold before it's flushed across network
	batchSize int
	// Stream to send blocks
	inputBlockStream <-chan *data.Block
	// callback function to send block
	sendBlock SendBlock
	// callback function to discontinue insertion
	cancelInsert CancelInsert
	// callBackResp allow client to take handle the packets coming from server aside from exception or end of stream
	callBackResp CallBackResp
	// lookout for response from server
	serverResponses <-chan response.Packet
	// rowsProcessed stores the total rows of data read and processed into blocks
	rowsSent int
	// done signal if the process is completed
	done chan struct{}
	// stores error of the insert process
	err error
	// callback function for logging
	logf Logf
}

func NewInsertProcess(sample *data.Block, sendBlock SendBlock, cancelInsert CancelInsert, opts ...InsertOption) *InsertProcess {
	newProcess := &InsertProcess{
		sample:       sample,
		sendBlock:    sendBlock,
		cancelInsert: cancelInsert,
		done:         make(chan struct{}),
	}

	for _, opt := range opts {
		opt(newProcess)
	}

	return newProcess
}

func (p *InsertProcess) Start(ctx context.Context,
	inputBlockStream <-chan *data.Block, serverResponseStream <-chan response.Packet) {

	p.inputBlockStream = inputBlockStream
	p.serverResponses = serverResponseStream
	p.startWatchingEvents(ctx)
}

func (p *InsertProcess) startWatchingEvents(ctx context.Context) {
	go func() {
		if p.callBackResp == nil {
			p.callBackResp = func(resp response.Packet) {}
		}

		defer close(p.done)

		if p.logf != nil {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			go p.startLogging(ctx)
		}

		p.err = p.watchEvents(ctx)
	}()
}

func (p *InsertProcess) startLogging(ctx context.Context) {
	var (
		timeStart            = time.Now()
		lastRecordedRowsSent int
	)

	defer func() {
		duration := time.Since(timeStart)
		averageSpeed := float64(p.rowsSent) / duration.Seconds()
		p.logf("total rows sent: %v, average speed = %v rows/s",
			utils.FormatCount(int64(p.rowsSent)), utils.FormatCount(int64(averageSpeed)),
		)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
			p.logf("%v total rows sent, %v rows/s", utils.FormatCount(int64(p.rowsSent)), utils.FormatCount(int64(p.rowsSent-lastRecordedRowsSent)))
			lastRecordedRowsSent = p.rowsSent
		}
	}
}

func (p *InsertProcess) watchEvents(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			p.cancelInsert()
			return context.Canceled

		case resp := <-p.serverResponses:
			switch resp := resp.(type) {
			case *response.ExceptionPacket:
				return resp
			case *response.EndOfStreamPacket:
				return nil
			default:
				p.callBackResp(resp)
			}
		case b, ok := <-p.inputBlockStream:
			if !ok {
				if err := p.sendBlock(&data.Block{}); err != nil {
					return err
				}
				p.inputBlockStream = nil
				continue
			}
			if b.NumRows == 0 {
				continue
			}
			if err := p.sendBlock(b); err != nil {
				return err
			}
			p.rowsSent += b.NumRows
			_ = b.Close()
		}
	}
}

func (p *InsertProcess) NumColumns() int {
	return p.sample.NumColumns
}

func (p *InsertProcess) Finish() (int, error) {
	<-p.done
	return p.rowsSent, p.err
}

func (p *InsertProcess) Error() error {
	return p.err
}

func (p *InsertProcess) BatchSize() int {
	return p.batchSize
}

func (p *InsertProcess) Sample() *data.Block {
	return p.sample
}
