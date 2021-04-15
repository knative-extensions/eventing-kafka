package config

import (
	"fmt"
	"hash/crc32"
)

func ConfigmapDataCheckSum(configMapData map[string]string) string {
	if configMapData == nil {
		return ""
	}
	configMapDataStr := fmt.Sprintf("%v", configMapData)
	checksum := fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(configMapDataStr)))
	return checksum
}
