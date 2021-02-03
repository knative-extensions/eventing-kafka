package reconciler

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"

	v1 "k8s.io/client-go/listers/core/v1"

	"knative.dev/eventing-kafka/pkg/source/control/certificates"
)

type TLSDialerFactory interface {
	GenerateTLSDialer(baseDialOptions *net.Dialer) (*tls.Dialer, error)
}

type ListerCertificateGetter struct {
	secrets   v1.SecretLister
	name      string
	namespace string
}

func NewCertificateGetter(secrets v1.SecretLister, namespace string, name string) *ListerCertificateGetter {
	return &ListerCertificateGetter{
		secrets:   secrets,
		name:      name,
		namespace: namespace,
	}
}

func (ch *ListerCertificateGetter) GenerateTLSDialer(baseDialOptions *net.Dialer) (*tls.Dialer, error) {
	secret, err := ch.secrets.Secrets(ch.namespace).Get(ch.name)
	if err != nil {
		return nil, err
	}
	if secret.Data == nil {
		return nil, fmt.Errorf("no tls configuration available")
	}
	caCertBytes := secret.Data[certificates.SecretCaCertKey]
	certBytes := secret.Data[certificates.SecretCertKey]
	privateKeyBytes := secret.Data[certificates.SecretPKKey]

	if caCertBytes == nil || certBytes == nil || privateKeyBytes == nil {
		return nil, fmt.Errorf("no tls configuration available")
	}
	controlPlaneCert, err := tls.X509KeyPair(certBytes, privateKeyBytes)
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caCertBytes)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{controlPlaneCert},
		RootCAs:      certPool,
		ServerName:   certificates.FakeDnsName,
	}

	// Copy from base dial options
	dialOptions := *baseDialOptions

	return &tls.Dialer{
		NetDialer: &dialOptions,
		Config:    tlsConfig,
	}, nil
}
