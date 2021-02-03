package certificates

const (
	Organization = "knative.dev"
	FakeDnsName  = "data-plane." + Organization

	SecretCaCertKey = "ca-cert.pem"
	SecretCertKey   = "public-cert.pem"
	SecretPKKey     = "private-key.pem"
)
