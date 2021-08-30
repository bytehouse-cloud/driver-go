package sdk

import (
	"bytes"
	"context"
	"io"

	"golang.org/x/sync/errgroup"

	"github.com/bytehouse-cloud/driver-go"
	"github.com/bytehouse-cloud/driver-go/conn"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/settings"
	"github.com/bytehouse-cloud/driver-go/driver/response"
	"github.com/bytehouse-cloud/driver-go/errors"
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
	InsertFromReader(ctx context.Context, query string, reader io.Reader) error
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

	return OpenConfig(config)
}

func OpenConfig(config *Config) (*Gateway, error) {
	newGatewayConn, err := conn.NewGatewayConn(
		config.connConfig,
		config.databaseName,
		config.authentication,
		//config.impersonation,
		config.compress,
		config.querySettings,
	)
	if err != nil {
		return nil, errors.ErrorfWithCaller("error getting new gateway connection = %v", err)
	}
	return &Gateway{Conn: newGatewayConn}, nil
}

func (g *Gateway) PrepareContext(ctx context.Context, query string) (Stmt, error) {
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
		conns[i], err = g.Clone()
		if err != nil {
			return nil, err
		}
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
	if err := g.SendInsertQuery(ctx, query); err != nil {
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

	return NewInsertStatement(ctx, sample, g.Conn.SendClientData, g.Conn.SendCancel, respStream,
		stream.OptionBatchSize(batchSize),
		stream.OptionAddCallBackResp(appendMeta),
	), nil
}

func (g *Gateway) InsertWithArgs(ctx context.Context, query string, args [][]interface{}, batchSize int) error {
	if len(args) == 0 {
		return errors.ErrorfWithCaller("nothing to insert")
	}

	stmt, err := g.PrepareInsert(ctx, query, batchSize)
	if err != nil {
		return err
	}

	for _, argsRow := range args {
		if err := stmt.ExecContext(ctx, argsRow...); err != nil {
			return err
		}
	}

	return stmt.Close()
}

func (g *Gateway) Closed() bool {
	return g.Conn.Closed()
}

func (g *Gateway) QueryContext(c context.Context, query string) (*QueryResult, error) {
	ctx := ContextWithCheckedConn(c)
	if utils.IsInsert(query) {
		iq, err := utils.ParseInsertQuery(query)
		if err != nil {
			return nil, err
		}

		return g.InsertWithData(ctx, iq.Query, bytes.NewReader([]byte(iq.Values)), iq.DataFmt, settings.DEFAULT_BLOCK_SIZE)
	}

	err := g.applySettingsFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	if g.Conn.InAnsiSQLMode() {
		return g.sendQueryAndGetResponse(ctx, query, nil, "")
	}

	return g.sendQueryAndGetResponse(ctx, query, nil, "")
}

func (g *Gateway) QueryContextWithExternalTable(c context.Context, query string, externalTable *ExternalTable) (*QueryResult, error) {
	ctx := ContextWithCheckedConn(c)
	err := g.applySettingsFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	eg, ctx := errgroup.WithContext(ctx)

	extTablesStream := make(chan *data.Block, 1)

	var qr *QueryResult

	eg.Go(func() error {
		newBlock, err := data.NewBlock(externalTable.columnNames, externalTable.columnTypes, len(externalTable.values))
		if err != nil {
			return err
		}

		columnValues := utils.TransposeMatrix(externalTable.values)
		for i, col := range newBlock.Columns {
			rowsRead, err := col.Data.ReadFromValues(columnValues[i])
			if err != nil {
				return errors.ErrorfWithCaller("failed to send block, col = %v, rowsRead = %d, ioErr = %v", col, rowsRead, err)
			}
		}

		extTablesStream <- newBlock
		close(extTablesStream)
		return nil
	})

	eg.Go(func() error {
		qr, err = g.sendQueryAndGetResponse(ctx, query, extTablesStream, externalTable.name)
		return err
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return qr, nil
}

func (g *Gateway) QueryContextWithExternalTableReader(c context.Context, query string, externalTable *ExternalTableReader) (*QueryResult, error) {
	ctx := ContextWithCheckedConn(c)
	err := g.applySettingsFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	extTableReader, err := format.BlockStreamFmtReaderFactory(externalTable.fileType, externalTable.reader, g.Conn.GetAllSettings())
	if err != nil {
		return nil, errors.ErrorfWithCaller("external table reader error = %v", err)
	}

	newBlock, err := data.NewBlock(externalTable.columnNames, externalTable.columnTypes, 0)
	if err != nil {
		return nil, err
	}
	extTablesStream := extTableReader.BlockStreamFmtRead(ctx, newBlock, settings.DEFAULT_BLOCK_SIZE)
	qr, err := g.sendQueryAndGetResponse(ctx, query, extTablesStream, externalTable.name)

	return qr, nil
}

func (g *Gateway) Query(query string) (*QueryResult, error) {
	return g.QueryContext(context.Background(), query)
}

// SendInsertQuery sends a prepared query to database
// Used before insertion of rows
func (g *Gateway) SendInsertQuery(ctx context.Context, query string) error {
	err := g.applySettingsFromCtx(ctx)
	if err != nil {
		return err
	}

	return g.Conn.SendQuery(query)
}

func (g *Gateway) InsertFromReader(ctx context.Context, query string, file io.Reader) error {
	qr, err := g.InsertWithDataFormatAuto(ctx, query, file)
	if err != nil {
		return err
	}
	defer qr.Close()

	return qr.Exception()
}

// InsertWithDataFormatAuto handles insert Query with data reader
func (g *Gateway) InsertWithDataFormatAuto(ctx context.Context, query string, dataReader io.Reader) (*QueryResult, error) {
	iq, err := utils.ParseInsertQuery(query)
	if err != nil {
		return nil, err
	}

	return g.InsertWithData(ctx, query, dataReader, iq.DataFmt, settings.DEFAULT_INSERT_BLOCK_SIZE)
}

func (g *Gateway) InsertWithData(ctx context.Context, query string, dataReader io.Reader, dataFmt string, blockSize int) (*QueryResult, error) {
	err := g.applySettingsFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	blockStreamReader, err := format.BlockStreamFmtReaderFactory(dataFmt, dataReader, g.Conn.GetAllSettings())
	if err != nil {
		return nil, err
	}

	if err = g.Conn.SendQuery(query); err != nil {
		return nil, err
	}
	respStreamForResult := make(chan response.Packet, 1)
	qr := NewInsertQueryResult(respStreamForResult)

	defer close(respStreamForResult)
	_, err = stream.HandleInsertFromFmtStream(ctx,
		g.Conn.GetResponseStream(ctx), blockStreamReader,
		g.Conn.SendClientData, g.Conn.SendCancel,
		func(resp response.Packet) {
			respStreamForResult <- resp
		},
		stream.OptionBatchSize(blockSize),
		stream.OptionAddLogf(g.Conn.Log),
	)

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
		select {
		case <-done:
			_ = g.Conn.SendCancel()
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
func (g *Gateway) applySettingsFromCtx(ctx context.Context) error {
	if queryContext, ok := ctx.(*bytehouse.QueryContext); ok {
		g.applyContext(queryContext)
	}

	return nil
}

func (g *Gateway) sendQueryAndGetResponse(ctx context.Context, sql string, extTables <-chan *data.Block, extTableName string) (*QueryResult, error) {
	if !HasCheckedConn(ctx) {
		if err := g.Conn.CheckConnection(); err != nil {
			return nil, err
		}
	}

	err := g.Conn.SendQueryWithExternalTable(sql, extTables, extTableName)
	if err != nil {
		return nil, err
	}

	responseStream := g.Conn.GetResponseStream(ctx)

	finish := g.listenCtxDone(ctx)
	qr := NewQueryResult(responseStream, finish)

	return qr, nil
}

// Ping exposed for SDK directly
func (g *Gateway) Ping() error {
	return g.Conn.CheckConnection()
}

// Close exposed for SDK directly
func (g *Gateway) Close() error {
	return g.Conn.Close()
}

// AddSetting exposed for SDK directly
func (g *Gateway) AddSetting(name string, value interface{}) error {
	return g.Conn.AddSetting(name, value)
}

func (g *Gateway) applyContext(queryContext *bytehouse.QueryContext) {
	for k, v := range queryContext.GetQuerySettings() {
		g.Conn.AddSettingsChecked(k, v)
	}
}

func (g *Gateway) Clone() (*Gateway, error) {
	newConn, err := g.Conn.Clone()
	if err != nil {
		return nil, err
	}
	return &Gateway{newConn}, nil
}

func HasCheckedConn(ctx context.Context) bool {
	if qc, ok := ctx.(*bytehouse.QueryContext); ok {
		return qc.GetCheckedConn()
	}

	return false
}

// ContextWithCheckedConn set HasCheckedConn as true to prevent checking connection validity again
func ContextWithCheckedConn(ctx context.Context) context.Context {
	qc, ok := ctx.(*bytehouse.QueryContext)
	if !ok {
		qc = bytehouse.NewQueryContext(ctx)
	}

	qc.SetCheckedConn(true)
	return qc
}

func resolveBatchSize(ctx context.Context) int {
	qc, ok := ctx.(*bytehouse.QueryContext)
	if !ok {
		return bytehouse.Default[bytehouse.InsertBlockSize].(int)
	}
	size, ok := qc.GetByteHouseSettings()[bytehouse.InsertBlockSize]
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
	count, ok := qc.GetByteHouseSettings()[bytehouse.InsertConnectionCount]
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
	parallelism, ok := qc.GetByteHouseSettings()[bytehouse.InsertBlockParallelism]
	if !ok {
		return bytehouse.Default[bytehouse.InsertBlockParallelism].(int)
	}
	return parallelism.(int)
}
