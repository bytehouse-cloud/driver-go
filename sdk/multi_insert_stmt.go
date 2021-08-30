package sdk

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
)

type closeAllConnection func()

// MultiInsertStatement that stores multiple Insert Statements attached to multiple connection.
// ExecContext can be done asynchronously. insertion load is distributed across insert stmts,
// switching from insert stmts happens when the columns buffer is at its full capacity(batch size).
type MultiInsertStatement struct {
	locker             sync.Locker
	stmtIdx, execCount int

	insertStmts []*InsertStmt
	close       closeAllConnection
}

func NewMultiInsertStatement(stmts []*InsertStmt, close closeAllConnection) (*MultiInsertStatement, error) {
	multiInsert := &MultiInsertStatement{
		locker:      &sync.Mutex{},
		insertStmts: stmts,
		close:       close,
	}

	return multiInsert, nil
}

func (m *MultiInsertStatement) ExecContext(ctx context.Context, args ...interface{}) error {
	m.locker.Lock()
	defer m.locker.Unlock()

	currentStmt := m.insertStmts[m.stmtIdx]

	m.execCount++
	if m.execCount == currentStmt.insertProcess.BatchSize() {
		defer m.Switch()
	}

	return currentStmt.ExecContext(ctx, args...)
}

func (m *MultiInsertStatement) Close() error {
	defer m.close()

	var eg errgroup.Group
	for _, insertStmt := range m.insertStmts {
		eg.Go(insertStmt.Close)
	}
	return eg.Wait()
}

func (m *MultiInsertStatement) Switch() {
	m.stmtIdx = (m.stmtIdx + 1) % len(m.insertStmts)
	m.execCount = 0
}
