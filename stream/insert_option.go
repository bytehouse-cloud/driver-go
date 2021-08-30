package stream

type InsertOption func(process *InsertProcess)

func OptionBatchSize(n int) InsertOption {
	return func(process *InsertProcess) {
		process.batchSize = n
	}
}

func OptionAddLogf(logf2 Logf) InsertOption {
	return func(process *InsertProcess) {
		process.logf = logf2
	}
}

func OptionAddCallBackResp(callback CallBackResp) InsertOption {
	return func(process *InsertProcess) {
		process.callBackResp = callback
	}
}
