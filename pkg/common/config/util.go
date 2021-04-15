package config

import (
	"fmt"
	"hash/crc32"

	corev1 "k8s.io/api/core/v1"
)

func ConfigmapDataCheckSum(configMap *corev1.ConfigMap) string {
	if configMap == nil || configMap.Data == nil {
		return ""
	}
	configMapDataStr := fmt.Sprintf("%v", configMap.Data)
	checksum := fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(configMapDataStr)))
	return checksum
}
