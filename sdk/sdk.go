package sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime/debug"

	"golang.org/x/sync/errgroup"

	"github.com/bytehouse-cloud/driver-go"
	"github.com/bytehouse-cloud/driver-go/conn"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/settings"
	"github.com/bytehouse-cloud/driver-go/driver/response"
	"github.com/bytehouse-cloud/driver-go/stream"
	"github.com/bytehouse-cloud/driver-go/stream/format"
	"github.com/bytehouse-cloud/driver-go/utils"
)

type Conn interface {
	// QueryContext sends a query and returns a QueryResult
	// The query result can be used to export to a file
	QueryContext(ctx context.Context, query string) (*QueryResult, error)
	// QueryContextWithExternalTable sends a query with an external table
	// The name of the external table in the query has to correspond to that in the externalTable you are sending
	QueryContextWithExternalTable(ctx context.Context, query string, externalTable *ExternalTable) (*QueryResult, error)
	// QueryContextWithExternalTable sends a query with an external table from an io.Reader (can be a file)
	// The name of the external table in the query has to correspond to that in the externalTable you are sending
	QueryContextWithExternalTableReader(ctx context.Context, query string, externalTable *ExternalTableReader) (*QueryResult, error)
	// PrepareContext is used for batch insertion
	// PrepareContext sends the query to the database and return a Stmt interface
	// The Stmt interface can be used to send the arguments for the query
	PrepareContext(ctx context.Context, query string) (Stmt, error)
	// InsertFromReader inserts data from io.Reader
	// Can be used for insert with files such as csv or json
	// DataPacket will be read from the reader until io.EOF is returned as an error from reader.Read()
	InsertFromReader(ctx context.Context, query string, reader io.Reader) (int, error)
}

type Stmt interface {
	// ExecContext is used to send a row of query argument to the clickhouse server
	ExecContext(ctx context.Context, args ...interface{}) error
	// Close sends leftover queued query arguments to the clickhouse server and closes the stmt
	// Close has to be called at the end for each Stmt
	Close() error
}

type Gateway struct {
	Conn *conn.GatewayConn
}

func Open(ctx context.Context, dsn string) (*Gateway, error) {
	var (
		logf        func(s string, a ...interface{})
		hostResolve func() (host string, err error)
	)

	if bytehouseCtx, ok := ctx.(*bytehouse.ConnectionContext); ok {
		logf = bytehouseCtx.GetLogf()
		hostResolve = bytehouseCtx.GetResolveHost()
	}

	config, err := ParseDSN(dsn, hostResolve, logf)
	if err != nil {
		return nil, err
	}
	return OpenConfig(config), nil
}

func OpenConfig(config *Config) *Gateway {
	newGatewayConn := conn.NewGatewayConn(
		config.connConfig,
		config.databaseName,
		config.authentication,
		//config.impersonation,
		config.compress,
		config.querySettings,
	)
	return &Gateway{Conn: newGatewayConn}
}

func (g *Gateway) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	insertQuery, err := utils.ParseInsertQuery(query)
	if err != nil {
		return nil, errors.New("PrepareContext is only valid for insertion query")
	}
	query = insertQuery.Query

	batchSize := resolveBatchSize(ctx)
	connCount := resolveConnCount(ctx)
	if connCount > 1 {
		return g.PrepareMultiConnectionInsert(ctx, query, batchSize, connCount)
	}

	return g.PrepareInsert(ctx, query, batchSize)
}

func (g *Gateway) PrepareMultiConnectionInsert(ctx context.Context, query string, batchSize, connCount int) (*MultiInsertStatement, error) {
	var err error
	stmts := make([]*InsertStmt, connCount)
	conns := make([]*Gateway, connCount)

	for i := range conns {
		conns[i] = g.Clone()
	}

	for i := range stmts {
		stmts[i], err = conns[i].PrepareInsert(ctx, query, batchSize)
		if err != nil {
			return nil, err
		}
	}

	closeAllConn := func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}

	return NewMultiInsertStatement(stmts, closeAllConn)
}

// PrepareInsert returns an Insert Statement that must be closed after use.
func (g *Gateway) PrepareInsert(ctx context.Context, query string, batchSize int) (*InsertStmt, error) {
	// Send first query
	if err := g.sendQuery(ctx, query); err != nil {
		return nil, err
	}

	respStream := g.Conn.GetResponseStream(ctx)

	var metaResult []response.Packet
	appendMeta := func(meta response.Packet) {
		metaResult = append(metaResult, meta)
	}

	sample, err := stream.CallBackUntilFirstBlock(ctx, respStream, appendMeta)
	if err != nil {
		return nil, err
	}

	return NewInsertStatement(ctx, sample, g.Conn.SendClientData, g.Conn.Cancel, respStream,
		stream.OptionBatchSize(batchSize),
		stream.OptionAddCallBackResp(appendMeta),
	), nil
}

func (g *Gateway) InsertArgs(ctx context.Context, query string, batchSize int, args ...interface{}) error {
	if len(args) == 0 {
		return errors.New("nothing to insert")
	}

	stmt, err := g.PrepareInsert(ctx, query, batchSize)
	if err != nil {
		return err
	}

	if err := stmt.ExecContext(ctx, args...); err != nil {
		return err
	}

	err = stmt.Close()
	return err
}

func (g *Gateway) InsertTable(ctx context.Context, query string, table [][]interface{}, batchSize int) error {
	flattened := make([]interface{}, 0, len(table)*len(table[0]))
	for _, rows := range table {
		for _, field := range rows {
			flattened = append(flattened, field)
		}
	}

	return g.InsertArgs(ctx, query, batchSize, flattened...)
}

func (g *Gateway) Closed() bool {
	return g.Conn.Closed()
}

func (g *Gateway) QueryContext(ctx context.Context, query string) (*QueryResult, error) {
	if utils.IsInsert(query) {
		iq, err := utils.ParseInsertQuery(query)
		if err != nil {
			return nil, err
		}
		return g.InsertWithData(ctx, iq.Query, bytes.NewReader([]byte(iq.Values)), iq.DataFmt, settings.DEFAULT_BLOCK_SIZE)
	}

	if err := g.sendQuery(ctx, query); err != nil {
		return nil, err
	}
	return g.streamResult(ctx)
}

func (g *Gateway) QueryContextWithExternalTableReader(ctx context.Context, query string, externalTable *ExternalTableReader) (*QueryResult, error) {
	extTableReader, err := format.BlockStreamFmtReaderFactory(externalTable.fileType, externalTable.reader, g.Conn.GetAllSettings())
	if err != nil {
		return nil, fmt.Errorf("external table reader error = %v", err)
	}

	newBlock, err := data.NewBlock(externalTable.columnNames, externalTable.columnTypes, 0)
	if err != nil {
		return nil, err
	}
	extTablesStream, yield := extTableReader.BlockStreamFmtRead(ctx, newBlock, settings.DEFAULT_BLOCK_SIZE)

	var eg errgroup.Group
	eg.Go(func() error {
		return g.sendQueryWithExternalTableStream(ctx, query, extTablesStream, externalTable.name)
	})
	eg.Go(func() error {
		_, err := yield()
		return err
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return g.streamResult(ctx)
}

func (g *Gateway) Query(query string) (*QueryResult, error) {
	return g.QueryContext(context.Background(), query)
}

// SendInsertQuery sends a prepared query to database
// Used before insertion of rows
func (g *Gateway) SendInsertQuery(ctx context.Context, query string) error {
	// make sure the insert statement is trimmed
	insertQuery, err := utils.ParseInsertQuery(query)
	if err != nil {
		return err
	} else {
		query = insertQuery.Query
	}

	_, err = g.InsertFromReader(ctx, query, bytes.NewBufferString(insertQuery.Values))
	return err

}

func (g *Gateway) InsertFromReader(ctx context.Context, query string, file io.Reader) (int, error) {
	qr, err := g.InsertWithDataFormatAuto(ctx, query, file)
	if err != nil {
		return 0, err
	}
	defer qr.Close()

	return qr.rowsInserted, qr.Exception()
}

// InsertWithDataFormatAuto handles insert Query with data reader
func (g *Gateway) InsertWithDataFormatAuto(ctx context.Context, query string, dataReader io.Reader) (*QueryResult, error) {
	iq, err := utils.ParseInsertQuery(query)
	if err != nil {
		return nil, err
	}

	blockSize := settings.DEFAULT_BLOCK_SIZE
	if bytehouseCtx, ok := ctx.(*bytehouse.QueryContext); ok {
		clientSetting := bytehouseCtx.GetQuerySettings()
		if declBlockSize, ok := clientSetting[bytehouse.InsertBlockSize]; ok {
			blockSize = declBlockSize.(int)
		}
	}

	return g.InsertWithData(ctx, query, dataReader, iq.DataFmt, blockSize)
}

func (g *Gateway) InsertWithData(ctx context.Context, query string, dataReader io.Reader, dataFmt string, blockSize int) (*QueryResult, error) {
	var settings map[string]interface{}
	if bytehouseCtx, ok := ctx.(*bytehouse.QueryContext); ok {
		settings = bytehouseCtx.GetQuerySettings()
	}
	blockStreamReader, err := format.BlockStreamFmtReaderFactory(dataFmt, dataReader, settings)
	if err != nil {
		return nil, err
	}

	if err = g.sendQuery(ctx, query); err != nil {
		return nil, err
	}
	respStreamForResult := make(chan response.Packet, 1)
	qr := NewInsertQueryResult(respStreamForResult)

	defer close(respStreamForResult)
	rowsInserted, err := stream.HandleInsertFromFmtStream(ctx,
		g.Conn.GetResponseStream(ctx), blockStreamReader,
		g.Conn.SendClientData, g.Conn.Cancel,
		func(resp response.Packet) {
			respStreamForResult <- resp
		},
		stream.OptionBatchSize(blockSize),
		stream.OptionAddLogf(g.Conn.Log),
	)
	qr.rowsInserted = rowsInserted

	if qr.err == nil {
		qr.err = err
	}

	return qr, nil
}

func (g *Gateway) listenCtxDone(ctx context.Context) func() {
	done := ctx.Done()
	if done == nil {
		return func() {}
	}

	finishSig := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		select {
		case <-done:
			g.Conn.Cancel()
		case <-finishSig:
			return
		}
	}()

	return func() {
		finishSig <- struct{}{}
	}
}

// applySettingsFromCtx is the setup method used to populate context and settings
// Must be used by all exported Query functions
func (g *Gateway) applySettingsFromCtx(ctx context.Context) func() {
	if queryContext, ok := ctx.(*bytehouse.QueryContext); ok {
		g.applyConnConfigs(queryContext.GetPersistentConnConfigs())
		revertConnConfigs := g.applyConnConfigsTemporarily(queryContext.GetTemporaryConnConfigs())
		revertQuerySettings := g.applySettingsTemporarily(queryContext.GetQuerySettings())
		return func() {
			revertConnConfigs()
			revertQuerySettings()
		}
	}
	return func() {}
}

func (g *Gateway) streamResult(ctx context.Context) (*QueryResult, error) {
	responseStream := g.Conn.GetResponseStream(ctx)
	finish := g.listenCtxDone(ctx)
	return NewQueryResult(responseStream, finish), nil
}

func (g *Gateway) sendQuery(ctx context.Context, query string) error {
	return g.sendQueryWithExternalTable(ctx, query, nil)
}

func (g *Gateway) QueryContextWithExternalTable(ctx context.Context, query string, externalTable *ExternalTable) (*QueryResult, error) {
	if err := g.sendQueryWithExternalTable(ctx, query, externalTable); err != nil {
		return nil, err
	}

	return g.streamResult(ctx)
}

func (g *Gateway) sendQueryWithExternalTable(ctx context.Context, query string, externalTable *ExternalTable) error {
	if externalTable == nil {
		return g.sendQueryWithExternalTableStream(ctx, query, nil, "")
	}

	extTablesStream, err := externalTable.ToSingleBlockStream()
	if err != nil {
		return err
	}
	return g.sendQueryWithExternalTableStream(ctx, query, extTablesStream, externalTable.name)
}

func (g *Gateway) sendQueryWithExternalTableStream(ctx context.Context, query string, extTableStream <-chan *data.Block, extTableName string) error {
	var queryID string
	if queryContext, ok := ctx.(*bytehouse.QueryContext); ok {
		queryID = queryContext.GetQueryID()
	}
	defer g.applySettingsFromCtx(ctx)()
	return g.Conn.SendQueryFull(query, queryID, extTableStream, extTableName)
}

// Ping exposed for SDK directly
func (g *Gateway) Ping() error {
	return g.Conn.CheckConnection()
}

// Close exposed for SDK directly
func (g *Gateway) Close() error {
	return g.Conn.Close()
}

func (g *Gateway) applySettingsTemporarily(m map[string]interface{}) func() {
	return g.Conn.AddSettingsTemporarily(m)
}

func (g *Gateway) applyConnConfigs(m map[string]interface{}) {
	g.Conn.ApplyConnConfigs(m)
}

func (g *Gateway) applyConnConfigsTemporarily(m map[string]interface{}) func() {
	return g.Conn.ApplyConnConfigsTemporarily(m)
}

func (g *Gateway) Clone() *Gateway {
	return &Gateway{g.Conn.Clone()}
}

func resolveBatchSize(ctx context.Context) int {
	qc, ok := ctx.(*bytehouse.QueryContext)
	if !ok {
		return bytehouse.Default[bytehouse.InsertBlockSize].(int)
	}
	size, ok := qc.GetClientSettings()[bytehouse.InsertBlockSize]
	if !ok {
		return bytehouse.Default[bytehouse.InsertBlockSize].(int)
	}
	return size.(int)
}

func resolveConnCount(ctx context.Context) int {
	// todo: a little repetitive from function above, do dynamic resolution base on type if needed
	qc, ok := ctx.(*bytehouse.QueryContext)
	if !ok {
		return bytehouse.Default[bytehouse.InsertConnectionCount].(int)
	}
	count, ok := qc.GetClientSettings()[bytehouse.InsertConnectionCount]
	if !ok {
		return bytehouse.Default[bytehouse.InsertConnectionCount].(int)
	}
	return count.(int)
}

func resolveInsertBlockParallelism(ctx context.Context) int {
	// todo: a little repetitive from function above, do dynamic resolution base on type if needed
	qc, ok := ctx.(*bytehouse.QueryContext)
	if !ok {
		return bytehouse.Default[bytehouse.InsertBlockParallelism].(int)
	}
	parallelism, ok := qc.GetClientSettings()[bytehouse.InsertBlockParallelism]
	if !ok {
		return bytehouse.Default[bytehouse.InsertBlockParallelism].(int)
	}
	return parallelism.(int)
}
