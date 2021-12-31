package conn

import (
	"errors"
	"strings"
)

const (
	RegionCnNorth1     = "CN-NORTH-1"
	RegionApSouthEast1 = "AP-SOUTHEAST-1"
)

var hostPortByRegion = map[string]string{
	RegionCnNorth1:     "gateway.aws-cn-north-1.bytehouse.cn:19000",
	RegionApSouthEast1: "gateway.aws-ap-southeast-1.bytehouse.cloud:19000",
}

func regionNotFound(region string) error {
	var sb strings.Builder
	sb.WriteString("region not found for: ")
	sb.WriteString(region)
	sb.WriteString("\navailable regions (case insensitive):")
	for k := range hostPortByRegion {
		sb.WriteString("\n\t")
		sb.WriteString(k)
	}
	return errors.New(sb.String())
}

func resolveRegion(region string) (hostPort string, err error) {
	var ok bool
	hostPort, ok = hostPortByRegion[strings.ToUpper(region)]
	if !ok {
		return "", regionNotFound(region)
	}
	return hostPort, nil
}
