package protocol

const (
	ClientHello = iota
	ClientQuery
	ClientData
	ClientCancel
	ClientPing

	// Check status of tables on the server
	ClientTablesStatusRequest

	// Keep the connection alive
	ClientKeepAlive

	// A block of data (compressed or not)
	ClientScalar

	// Customized for token-auth (for users)
	ClientHelloToken

	ClientHelloImpersonation
	_
	ClientQueryPlan
	ClientSystemHello = 1000
)

// compress
const (
	CompressEnable  uint64 = 1
	CompressDisable uint64 = 0
)

// Query Stage
const (
	StageFetchColumns       = 0 /// Only read/have been read the columns specified in the query.
	StageWithMergeableState = 1 /// Until the stage where the results of processing on different servers can be combined.
	StageComplete           = 2 /// Completely
)

// Query Kind
const (
	NoQuery = iota /// Uninitialized object.
	InitialQuery
	SecondaryQuery /// Query that was initiated by another query for distributed or ON CLUSTER query execution.
)

// Interface
const (
	TCP = iota + 1
	HTTP
)
