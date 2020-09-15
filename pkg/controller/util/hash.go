package util

import (
	"crypto/md5"
	"fmt"
)

// Generate An MD5 Hash Of A String And Return Desired Number Of Characters
func GenerateHash(stringToHash string, length int) string {
	// Create an MD5 hash and return however many characters the caller wants (note that the max is 32 for MD5)
	return fmt.Sprintf(fmt.Sprintf("%%.%ds", length), fmt.Sprintf("%x", md5.Sum([]byte(stringToHash))))
}
