package conn

func (g *GatewayConn) writeString(s string) error {
	return g.encoder.String(s)
}

func (g *GatewayConn) writeUvarint(u uint64) error {
	return g.encoder.Uvarint(u)
}

func (g *GatewayConn) readUvariant() (uint64, error) {
	return g.decoder.Uvarint()
}
