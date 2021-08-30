package conn

import (
	"errors"
	"strings"
)

type Region string

const (
	CnNorth1     = "CN-NORTH-1"
	ApSouthEast1 = "AP-SOUTHEAST-1"
)

var hostPortByRegion = map[Region]string{
	CnNorth1:     "gateway.aws-cn-north-1.bytehouse.cn:19000",
	ApSouthEast1: "gateway.aws-ap-southeast-1.bytehouse.cloud:19000",
}

func regionNotFound(region Region) error {
	var sb strings.Builder
	sb.WriteString("region not found for: ")
	sb.WriteString(string(region))
	sb.WriteString("\navailable regions:")
	for k := range hostPortByRegion {
		sb.WriteString("\n\t")
		sb.WriteString(string(k))
	}
	return errors.New(sb.String())
}

func resolveRegion(region Region) (hostPort string, err error) {
	var ok bool
	hostPort, ok = hostPortByRegion[region]
	if !ok {
		return "", regionNotFound(region)
	}
	return hostPort, nil
}
