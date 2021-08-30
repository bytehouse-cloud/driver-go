package bytepool

// GetBytes gets a buffer with at least of capacity as defined by caller.
// panics with negative value
func GetBytes(length, capacity int) []byte {
	idx := getMinIdx(capacity)
	chosenPool := getExp2Pool(idx)
	select {
	case b := <-chosenPool.bytesStream:
		go postGet(b, chosenPool)
		return b[:length]
	default:
		capacity = 1 << idx
		capacity >>= 1
		return make([]byte, length, capacity)
	}
}

func GetBytesWithLen(length int) []byte {
	return GetBytes(length, length)
}

// PutBytes recycles the buffer to the bytepool
func PutBytes(buf []byte) {
	if !withinLimitWhenAdd(buf) {
		return
	}
	go putBytes(buf)
}

func PutBytesStream(bytesStream <-chan []byte) {
	for {
		select {
		case buf, ok := <-bytesStream:
			if !ok {
				return
			}
			PutBytes(buf)
		default:
			return
		}
	}
}

func postGet(b []byte, chosenPool *nPool) {
	decrementMemUsage(cap(b))
	chosenPool.monitor()
}

func putBytes(buf []byte) {
	incrementMemUsage(cap(buf))
	idx := getMaxIdx(cap(buf))
	chosenPool := exp2Pools[idx]
	select {
	case chosenPool.bytesStream <- buf:
	default:
		chosenPool.latePut(buf)
	}
	chosenPool.monitor()
}

// getMinIdx returns the index to access exp2Pool based on given capacity,
// which the pool accessed will contain []byte of with at least given capacity.
func getMinIdx(capacity int) int {
	if capacity < 2 {
		return capacity
	}

	capacity--
	result := 1
	for capacity > 0 {
		result++
		capacity >>= 1
	}
	return result
}

// getMaxIdx returns the index to access exp2Pool based on given capacity,
// which the pool accessed will contain []byte up to given capacity.
func getMaxIdx(capacity int) int {
	if capacity < 2 {
		return capacity
	}

	var result int
	for capacity > 0 {
		result++
		capacity >>= 1
	}
	return result
}
