package data

import (
	"strconv"
	"strings"
	"time"
	// to support timezone in windows
	// https://github.com/golang/go/issues/38453
	_ "time/tzdata"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/protocol"
)

type ServerInfo struct {
	Name         string
	Revision     uint64
	MinorVersion uint64
	MajorVersion uint64
	Timezone     *time.Location

	//Additional information since we upgraded the client revision number to 54406 (DBMS_MIN_REVISION_WITH_SERVER_LOGS)
	DisplayName  string
	VersionPatch uint64
}

func ReadServerInfo(decoder *ch_encoding.Decoder) (*ServerInfo, error) {
	var (
		serverInfo ServerInfo
		err        error
	)
	if serverInfo.Name, err = decoder.String(); err != nil {
		return nil, err
	}
	serverInfo.MajorVersion, err = decoder.Uvarint()
	if err != nil {
		return nil, err
	}
	serverInfo.MinorVersion, err = decoder.Uvarint()
	if err != nil {
		return nil, err
	}
	serverInfo.Revision, err = decoder.Uvarint()
	if err != nil {
		return nil, err

	}

	if serverInfo.Revision >= protocol.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
		timezoneString, err := decoder.String()
		if err != nil {
			return nil, err
		}
		if serverInfo.Timezone, err = time.LoadLocation(timezoneString); err != nil {
			return nil, err
		}
	}
	if serverInfo.Revision >= protocol.DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
		if serverInfo.DisplayName, err = decoder.String(); err != nil {
			return nil, err
		}
	}
	if serverInfo.Revision >= protocol.DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		if serverInfo.VersionPatch, err = decoder.Uvarint(); err != nil {
			return nil, err
		}
	}
	return &serverInfo, nil
}

func (s *ServerInfo) String() string {
	var b strings.Builder
	b.WriteString("Server Information: ")
	b.WriteString(s.Name)
	b.WriteByte(space)
	b.WriteString(strconv.FormatUint(s.MajorVersion, 10))
	b.WriteByte(dot)
	b.WriteString(strconv.FormatUint(s.MinorVersion, 10))
	b.WriteByte(dot)
	b.WriteString(strconv.FormatUint(s.Revision, 10))
	if s.VersionPatch != 0 {
		b.WriteString(", Patch ")
		b.WriteString(strconv.FormatUint(s.VersionPatch, 10))
	}
	return b.String()
}

const (
	dot   = '.'
	space = ' '
)
