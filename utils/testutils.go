package utils

import "testing"

const AllowMapSQLScript = "set allow_experimental_map_type = 1"

func SkipIntegrationTestIfShort(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping integration test")
	}
}
