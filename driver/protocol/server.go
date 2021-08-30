package protocol

const (
	ServerHello = iota
	ServerData
	ServerException
	ServerProgress
	ServerPong
	ServerEndOfStream
	ServerProfileInfo
	ServerTotals
	ServerExtremes
	ServerTablesStatus
	ServerLog
	ServerTableColumns
	ServerQueryPlan
	ServerAggQueryPlan
	ServerQueryMetadata
)
