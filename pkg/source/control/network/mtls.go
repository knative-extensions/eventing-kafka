package network

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"time"

	"go.uber.org/zap"
	"knative.dev/pkg/logging"
)

const (
	organization = "knative.dev"
	fakeDnsName  = "data-plane." + organization
)

var randReader = rand.Reader
var serialNumberLimit = new(big.Int).Lsh(big.NewInt(1), 128)

// Copy-pasted from https://github.com/knative/pkg/blob/975a1cf9e4470b26ce54d9cc628dbd50716b6b95/webhook/certificates/resources/certs.go
func createCertTemplate() (*x509.Certificate, error) {
	serialNumber, err := rand.Int(randReader, serialNumberLimit)
	if err != nil {
		return nil, errors.New("failed to generate serial number: " + err.Error())
	}

	tmpl := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{organization},
			CommonName:   "control-plane",
		},
		SignatureAlgorithm:    x509.SHA256WithRSA,
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(20, 0, 0),
		BasicConstraintsValid: true,
		DNSNames:              []string{fakeDnsName},
	}
	return &tmpl, nil
}

// Create cert template suitable for CA and hence signing
func createCACertTemplate() (*x509.Certificate, error) {
	rootCert, err := createCertTemplate()
	if err != nil {
		return nil, err
	}
	// Make it into a CA cert and change it so we can use it to sign certs
	rootCert.IsCA = true
	rootCert.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature
	rootCert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	return rootCert, nil
}

// Create cert template that we can use on the server for TLS
func createServerCertTemplate() (*x509.Certificate, error) {
	serverCert, err := createCertTemplate()
	if err != nil {
		return nil, err
	}
	serverCert.KeyUsage = x509.KeyUsageDigitalSignature
	serverCert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	return serverCert, err
}

// Create cert template that we can use on the server for TLS
func createClientCertTemplate() (*x509.Certificate, error) {
	serverCert, err := createCertTemplate()
	if err != nil {
		return nil, err
	}
	serverCert.KeyUsage = x509.KeyUsageDigitalSignature
	serverCert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	return serverCert, err
}

func createCert(template, parent *x509.Certificate, pub, parentPriv interface{}) (cert *x509.Certificate, certPEM *pem.Block, err error) {
	certDER, err := x509.CreateCertificate(rand.Reader, template, parent, pub, parentPriv)
	if err != nil {
		return
	}
	cert, err = x509.ParseCertificate(certDER)
	if err != nil {
		return
	}
	certPEM = &pem.Block{Type: "CERTIFICATE", Bytes: certDER}
	return
}

func CreateCA(ctx context.Context) (*rsa.PrivateKey, *x509.Certificate, *pem.Block, error) {
	logger := logging.FromContext(ctx)
	rootKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		logger.Errorw("error generating random key", zap.Error(err))
		return nil, nil, nil, err
	}

	rootCertTmpl, err := createCACertTemplate()
	if err != nil {
		logger.Errorw("error generating CA cert", zap.Error(err))
		return nil, nil, nil, err
	}

	rootCert, caCertPem, err := createCert(rootCertTmpl, rootCertTmpl, &rootKey.PublicKey, rootKey)
	if err != nil {
		logger.Errorw("error signing the CA cert", zap.Error(err))
		return nil, nil, nil, err
	}
	return rootKey, rootCert, caCertPem, nil
}

func CreateControlPlaneCert(ctx context.Context, caKey *rsa.PrivateKey, caCertificate *x509.Certificate) (*KeyPair, error) {
	logger := logging.FromContext(ctx)

	// Then create the private key for the serving cert
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		logger.Errorw("error generating random key", zap.Error(err))
		return nil, err
	}
	clientCertTemplate, err := createClientCertTemplate()
	if err != nil {
		logger.Errorw("failed to create the server certificate template", zap.Error(err))
		return nil, err
	}

	// create a certificate which wraps the server's public key, sign it with the CA private key
	_, clientCertPEM, err := createCert(clientCertTemplate, caCertificate, &clientKey.PublicKey, caKey)
	if err != nil {
		logger.Errorw("error signing client certificate template", zap.Error(err))
		return nil, err
	}
	privateClientKeyPEM := &pem.Block{
		Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey),
	}
	return newKeyPair(privateClientKeyPEM, clientCertPEM), nil
}

func CreateDataPlaneCert(ctx context.Context, caKey *rsa.PrivateKey, caCertificate *x509.Certificate) (*KeyPair, error) {
	logger := logging.FromContext(ctx)

	// Then create the private key for the serving cert
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		logger.Errorw("error generating random key", zap.Error(err))
		return nil, err
	}
	serverCertTemplate, err := createServerCertTemplate()
	if err != nil {
		logger.Errorw("failed to create the server certificate template", zap.Error(err))
		return nil, err
	}

	// create a certificate which wraps the server's public key, sign it with the CA private key
	_, serverCertPEM, err := createCert(serverCertTemplate, caCertificate, &serverKey.PublicKey, caKey)
	if err != nil {
		logger.Errorw("error signing server certificate template", zap.Error(err))
		return nil, err
	}
	privateServerKeyPEM := &pem.Block{
		Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey),
	}
	return newKeyPair(privateServerKeyPEM, serverCertPEM), nil
}

type KeyPair struct {
	privateKeyBlock    *pem.Block
	privateKeyPemBytes []byte

	certBlock *pem.Block
	certBytes []byte
}

func newKeyPair(privateKey *pem.Block, cert *pem.Block) *KeyPair {
	return &KeyPair{
		privateKeyBlock:    privateKey,
		privateKeyPemBytes: pem.EncodeToMemory(privateKey),
		certBlock:          cert,
		certBytes:          pem.EncodeToMemory(cert),
	}
}

func (kh *KeyPair) PrivateKey() *pem.Block {
	return kh.privateKeyBlock
}

func (kh *KeyPair) PrivateKeyBytes() []byte {
	return kh.privateKeyPemBytes
}

func (kh *KeyPair) Cert() *pem.Block {
	return kh.certBlock
}

func (kh *KeyPair) CertBytes() []byte {
	return kh.certBytes
}

type CertificateManager struct {
	caPrivateKey *rsa.PrivateKey
	caCert       *x509.Certificate

	caCertPem   *pem.Block
	caCertBytes []byte

	controllerKeyPair *KeyPair
}

func NewCertificateManager(ctx context.Context) (*CertificateManager, error) {
	caPrivateKey, caCert, caCertPem, err := CreateCA(ctx)
	if err != nil {
		return nil, err
	}
	controllerKeyPair, err := CreateControlPlaneCert(ctx, caPrivateKey, caCert)
	if err != nil {
		return nil, err
	}
	return &CertificateManager{
		caPrivateKey:      caPrivateKey,
		caCert:            caCert,
		caCertPem:         caCertPem,
		caCertBytes:       pem.EncodeToMemory(caCertPem),
		controllerKeyPair: controllerKeyPair,
	}, nil
}

func (cm *CertificateManager) CaCert() *x509.Certificate {
	return cm.caCert
}

func (cm *CertificateManager) CaCertPem() *pem.Block {
	return cm.caCertPem
}

func (cm *CertificateManager) CaCertBytes() []byte {
	return cm.caCertBytes
}

func (cm *CertificateManager) EmitNewDataPlaneCertificate(ctx context.Context) (*KeyPair, error) {
	return CreateDataPlaneCert(ctx, cm.caPrivateKey, cm.caCert)
}

func (cm *CertificateManager) GenerateTLSDialer(baseDialOptions *net.Dialer) (*tls.Dialer, error) {
	caCert := cm.caCert
	controlPlaneKeyPair := cm.controllerKeyPair

	controlPlaneCert, err := tls.X509KeyPair(controlPlaneKeyPair.CertBytes(), controlPlaneKeyPair.PrivateKeyBytes())
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	certPool.AddCert(caCert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{controlPlaneCert},
		RootCAs:      certPool,
		ServerName:   fakeDnsName,
	}

	// Copy from base dial options
	dialOptions := *baseDialOptions

	return &tls.Dialer{
		NetDialer: &dialOptions,
		Config:    tlsConfig,
	}, nil
}
