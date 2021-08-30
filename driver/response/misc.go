package response

type HelloPacket struct{}

func (s *HelloPacket) Close() error {
	return nil
}

func (s *HelloPacket) String() string {
	return hello
}

func (s *HelloPacket) packet() {}

type PongPacket struct{}

func (p *PongPacket) Close() error {
	return nil
}

func (p *PongPacket) String() string {
	return pong
}

func (p *PongPacket) packet() {}

type EndOfStreamPacket struct{}

func (s *EndOfStreamPacket) Close() error {
	return nil
}

func (s *EndOfStreamPacket) String() string {
	return endOfStream
}

func (s *EndOfStreamPacket) packet() {}

const (
	hello       = "Hello"
	pong        = "Pong"
	endOfStream = "End of Stream"
)
