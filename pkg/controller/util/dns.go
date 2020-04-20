package util

import (
	"regexp"
	"strings"
)

// Compiled RegExps
var startsWithLowercaseAlphaCharRegExp = regexp.MustCompile("^[a-z].*$")
var endsWithLowercaseAlphaCharRegExp = regexp.MustCompile("^.*[a-z]$")
var invalidK8sServiceCharactersRegExp = regexp.MustCompile("[^a-z0-9\\-]+")

// Return A Valid DNS Name Which Is As Close To The Specified Name As Possible & Truncated To The Smaller Of Specified Length / 63
func GenerateValidDnsName(name string, length int, prefix bool, suffix bool) string {

	// Max Truncation Length Is 63
	if length <= 0 || length > 63 {
		length = 63
	}

	// Convert To LowerCase
	validDnsName := strings.ToLower(name)

	// Strip Any Invalid DNS Characters
	validDnsName = invalidK8sServiceCharactersRegExp.ReplaceAllString(validDnsName, "")

	// Prepend Alpha Prefix If Needed
	if prefix && !startsWithLowercaseAlphaCharRegExp.MatchString(validDnsName) {
		validDnsName = "kk-" + validDnsName
	}

	// Truncate If Too Long
	if len(validDnsName) > length {
		validDnsName = validDnsName[:length]
	}

	// Remove Any Trailing Non Alpha
	for suffix && !endsWithLowercaseAlphaCharRegExp.MatchString(validDnsName) {
		validDnsName = validDnsName[:(len(validDnsName) - 1)]
	}

	// Return The Valid DNS Name
	return validDnsName
}
