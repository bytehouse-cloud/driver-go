package stream

type AsyncOption func(process AsyncToBlockProcess)

type parallelismSetter interface {
	setParallelism(int)
}

func OptionSetParallelism(n int) AsyncOption {
	return func(process AsyncToBlockProcess) {
		if setter, ok := process.(parallelismSetter); ok {
			setter.setParallelism(n)
		}
	}
}

type recycleSetter interface {
	setRecycle(RecycleColumnValues)
}

func OptionSetRecycle(recycle RecycleColumnValues) AsyncOption {
	return func(process AsyncToBlockProcess) {
		if setter, ok := process.(recycleSetter); ok {
			setter.setRecycle(recycle)
		}
	}
}
