package conn

import (
	"errors"
	"strings"
)

const (
	RegionCnBeijing = "CN-BEIJING"
)

var hostPortByVolcRegion = map[string]string{
	RegionCnBeijing: "bytehouse-cn-beijing.volces.com:19000",
}

func resolveVolcanoRegion(region string) (hostPort string, err error) {
	var ok bool
	hostPort, ok = hostPortByVolcRegion[strings.ToUpper(region)]
	if !ok {
		return "", volcanoRegionNotFound(region)
	}
	return hostPort, nil
}

func volcanoRegionNotFound(region string) error {
	var sb strings.Builder
	sb.WriteString("volcano region not found for: ")
	sb.WriteString(region)
	sb.WriteString("\navailable regions (case insensitive):")
	for k := range hostPortByVolcRegion {
		sb.WriteString("\n\t")
		sb.WriteString(k)
	}
	return errors.New(sb.String())
}
