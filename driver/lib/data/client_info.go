package data

import (
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const ClientName = "Enhanced Golang SQLDriver"

const (
	//ClickHouseRevision         = 54213
	ClickHouseRevision = 54406 //To receive server logs, we have to update the client revision
	//ClickHouseRevision         = protocol.DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO //To receive TableColumn Metadata which is being expected by clickhouse-client
	//ClickHouseDBMSVersionMajor = 1
	ClickHouseDBMSVersionMajor = 0
	ClickHouseDBMSVersionMinor = 1
)

func WriteClientInfo(encoder *ch_encoding.Encoder) (err error) {
	if err = encoder.Uvarint(ClickHouseDBMSVersionMajor); err != nil {
		return err
	}
	if err = encoder.Uvarint(ClickHouseDBMSVersionMinor); err != nil {
		return err
	}
	if err = encoder.Uvarint(ClickHouseRevision); err != nil {
		return err
	}
	return err
}
