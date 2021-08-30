package settings

const (
	DBMS_DEFAULT_CONNECT_TIMEOUT_SEC              = 10
	DBMS_DEFAULT_CONNECT_TIMEOUT_WITH_FAILOVER_MS = 50
	DBMS_DEFAULT_SEND_TIMEOUT_SEC                 = 300
	DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC              = 300

	/// Timeout for synchronous request-result protocol call (like Ping or TablesStatus).
	DBMS_DEFAULT_POLL_INTERVAL = 10

	/// The size of the I/O buffer by default.
	DBMS_DEFAULT_BUFFER_SIZE = 1048576

	/** Which blocks by default read the data (by number of rows).
	 * Smaller values give better cache locality, less consumption of RAM, but more overhead to process the query.
	 */
	DEFAULT_BLOCK_SIZE = 65536

	/** Which blocks should be formed for insertion into the table, if we control the formation of blocks.
	 * (Sometimes the blocks are inserted exactly such blocks that have been read / transmitted from the outside, and this parameter does not affect their size.)
	 * More than DEFAULT_BLOCK_SIZE, because in some tables a block of data on the disk is created for each block (quite a big thing),
	 *  and if the parts were small, then it would be costly then to combine them.
	 */
	DEFAULT_INSERT_BLOCK_SIZE = 1048576
	DEFAULT_BLOCK_SIZE_BYTES  = 0

	DBMS_DEFAULT_DISTRIBUTED_CONNECTIONS_POOL_SIZE       = 1024
	DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES = 3

	DBMS_MIN_REVISION_WITH_TABLES_STATUS = 54226

	DEFAULT_HTTP_READ_BUFFER_TIMEOUT            = 1800
	DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT = 1

	//Unconfirmed Setting
	LoadBalancing                           = "random"
	MERGE_TREE_MIN_ROWS_FOR_CONCURRENT_READ = 20 * 8192
	AFTER_HAVING_EXCLUSIVE                  = ""
	MERGE_TREE_MAX_ROWS_TO_USE_CACHE        = 1024 * 1024
	DistributedProductMode                  = "deny"
	read_overflow_mode                      = "throw"
	group_by_overflow_mode                  = "throw"
	sort_overflow_mode                      = "throw"
	result_overflow_mode                    = "throw"
	timeout_overflow_mode                   = "throw"
	set_overflow_mode                       = "throw"
	join_overflow_mode                      = "throw"
	transfer_overflow_mode                  = "throw"
	distinct_overflow_mode                  = "throw"
	HTTP_MAX_MULTIPART_FORM_DATA_SIZE       = 1024 * 1024 * 1024
	QUERY_AUTO_RETRY_MILLISECOND            = 300 * 1000
)
