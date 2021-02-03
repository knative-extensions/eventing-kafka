package certificates

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
)

type KeyPair struct {
	privateKeyBlock    *pem.Block
	privateKeyPemBytes []byte

	certBlock    *pem.Block
	certPemBytes []byte
}

func NewKeyPair(privateKey *pem.Block, cert *pem.Block) *KeyPair {
	return &KeyPair{
		privateKeyBlock:    privateKey,
		privateKeyPemBytes: pem.EncodeToMemory(privateKey),
		certBlock:          cert,
		certPemBytes:       pem.EncodeToMemory(cert),
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
	return kh.certPemBytes
}

func (kh *KeyPair) Parse() (*x509.Certificate, *rsa.PrivateKey, error) {
	return ParseCert(kh.certPemBytes, kh.privateKeyPemBytes)
}
