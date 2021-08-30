package bytehouse

const (
	InsertBlockSize        = "insert_block_size"
	InsertBlockParallelism = "insert_block_parallelism"
	InsertConnectionCount  = "insert_connection_count"
)

var Default = map[string]interface{}{
	InsertBlockSize:        65536,
	InsertConnectionCount:  1,
	InsertBlockParallelism: 1,
}
