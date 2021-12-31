package response

import (
	"time"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/protocol"
	"github.com/bytehouse-cloud/driver-go/errors"
)

type Packet interface {
	packet()
	String() string
	Close() error
}

const unknownPacketType = "unknown packet type: %v"

func ReadPacket(decoder *ch_encoding.Decoder, compress bool, revision uint64) (Packet, error) {
	return ReadPacketWithLocation(decoder, compress, revision, nil)
}

func ReadPacketWithLocation(decoder *ch_encoding.Decoder, compress bool, revision uint64, location *time.Location) (Packet, error) {
	packetType, err := decoder.Uvarint()
	if err != nil {
		return nil, err
	}
	switch packetType {
	case protocol.ServerHello:
		return &HelloPacket{}, nil
	case protocol.ServerData:
		return readDataPacket(decoder, compress, location)
	case protocol.ServerException:
		return readExceptionPacket(decoder)
	case protocol.ServerProgress:
		return readProgressPacket(decoder, revision)
	case protocol.ServerPong:
		return &PongPacket{}, nil
	case protocol.ServerEndOfStream:
		return &EndOfStreamPacket{}, nil
	case protocol.ServerProfileInfo:
		return readProfilePacket(decoder)
	case protocol.ServerTotals:
		return readTotalsPacket(decoder, compress, location)
	case protocol.ServerExtremes:
		return readExtremesPacket(decoder, compress, location)
	case protocol.ServerTablesStatus:
		return readTableStatusPacket(decoder)
	case protocol.ServerLog:
		return readLogPacket(decoder, location)
	case protocol.ServerTableColumns:
		return readTableColumnsPacket(decoder)
	case protocol.ServerQueryPlan:
		return readQueryPlanPacket(decoder)
	case protocol.ServerAggQueryPlan:
		return readAggQueryPlanPacket(decoder)
	case protocol.ServerQueryMetadata:
		return readQueryMetadataPacket(decoder)
	default:
		return nil, errors.ErrorfWithCaller(unknownPacketType, packetType)
	}
}

func WritePacket(p Packet, encoder *ch_encoding.Encoder, compress bool, revision uint64) (err error) {
	switch p := p.(type) {
	case *HelloPacket:
		return encoder.Uvarint(protocol.ServerHello)
	case *DataPacket:
		if err = encoder.Uvarint(protocol.ServerData); err != nil {
			return err
		}
		return writeDataPacket(p, encoder, compress)
	case *ExceptionPacket:
		if err = encoder.Uvarint(protocol.ServerException); err != nil {
			return err
		}
		return writeExceptionPacket(p, encoder)
	case *ProgressPacket:
		if err = encoder.Uvarint(protocol.ServerProgress); err != nil {
			return err
		}
		return writeProgressPacket(p, encoder, revision)
	case *PongPacket:
		return encoder.Uvarint(protocol.ServerPong)
	case *EndOfStreamPacket:
		return encoder.Uvarint(protocol.ServerEndOfStream)
	case *ProfilePacket:
		if err = encoder.Uvarint(protocol.ServerProfileInfo); err != nil {
			return err
		}
		return writeProfilePacket(p, encoder)
	case *TotalsPacket:
		if err = encoder.Uvarint(protocol.ServerTotals); err != nil {
			return err
		}
		return writeTotalsPacket(p, encoder, compress)
	case *ExtremesPacket:
		if err = encoder.Uvarint(protocol.ServerExtremes); err != nil {
			return err
		}
		return writeExtremesPacket(p, encoder, compress)
	case *tableStatusPacket:
		if err = encoder.Uvarint(protocol.ServerTablesStatus); err != nil {
			return err
		}
		return writeTableStatusPacket(p, encoder)
	case *LogPacket:
		if err = encoder.Uvarint(protocol.ServerLog); err != nil {
			return err
		}
		return writeLogPacket(p, encoder)
	case *TableColumnsPacket:
		if err = encoder.Uvarint(protocol.ServerTableColumns); err != nil {
			return err
		}
		return writeTableColumnsPacket(p, encoder)
	case *QueryPlanPacket:
		if err = encoder.Uvarint(protocol.ServerQueryPlan); err != nil {
			return err
		}
		return writeQueryPlanPacket(p, encoder)
	case *AggregateQueryPlanPacket:
		if err = encoder.Uvarint(protocol.ServerAggQueryPlan); err != nil {
			return err
		}
		return writeAggQueryPlanPacket(p, encoder)
	case *QueryMetadataPacket:
		if err = encoder.Uvarint(protocol.ServerQueryMetadata); err != nil {
			return err
		}
		return writeQueryMetadataPacket(p, encoder)
	default:
		return errors.ErrorfWithCaller("unknown packet type: %T", p)
	}
}
