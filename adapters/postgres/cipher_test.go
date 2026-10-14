package postgres

import (
	"bytes"
	"testing"
)

func TestTenantBoundCipher(t *testing.T) {
	c, _ := NewAESGCM(bytes.Repeat([]byte{1}, 32))
	b, _ := c.Seal("tenant1", []byte("private"))
	if _, err := c.Open("tenant2", b); err == nil {
		t.Fatal("cross-tenant decryption")
	}
	b[len(b)-1] ^= 1
	if _, err := c.Open("tenant1", b); err == nil {
		t.Fatal("tamper not detected")
	}
}
