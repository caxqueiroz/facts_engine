package postgres

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
)

// Cipher encrypts retained payloads. Production implementations should use envelope
// encryption and bank-managed versioned keys; destroying one shared key is not selective retention.
type Cipher interface {
	Seal(tenant string, plaintext []byte) ([]byte, error)
	Open(tenant string, ciphertext []byte) ([]byte, error)
}
type AESGCM struct{ aead cipher.AEAD }

func NewAESGCM(key []byte) (*AESGCM, error) {
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	a, err := cipher.NewGCM(b)
	if err != nil {
		return nil, err
	}
	return &AESGCM{a}, nil
}
func (a *AESGCM) Seal(t string, p []byte) ([]byte, error) {
	n := make([]byte, a.aead.NonceSize())
	if _, err := rand.Read(n); err != nil {
		return nil, err
	}
	return a.aead.Seal(n, n, p, []byte(t)), nil
}
func (a *AESGCM) Open(t string, b []byte) ([]byte, error) {
	n := a.aead.NonceSize()
	if len(b) < n {
		return nil, fmt.Errorf("invalid encrypted payload")
	}
	return a.aead.Open(nil, b[:n], b[n:], []byte(t))
}
