package conn

const (
	TCP                  uint64 = 1
	expectedServerHello         = "expected server hello, got %s instead"
	unknownQuerySetting         = "unknown query setting, %s"
	serverResponseStream        = "Server Response Stream"

	readOnCloseRefreshReader  = "read on closed refresh reader: channel closed"
	closeOnCloseRefreshReader = "close on a closed refresh reader"
)
