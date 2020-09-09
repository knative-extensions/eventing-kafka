package k8s

const (
	K8sLabelValueMaxLength = 63
)

// Truncate The Specified K8S Label Value To The Max
func TruncateLabelValue(str string) string {
	strCopy := str
	if len(str) > K8sLabelValueMaxLength {
		strCopy = str[0:K8sLabelValueMaxLength]
	}
	return strCopy
}
