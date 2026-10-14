// Package runtimeconfig contains bounded deployment configuration helpers.
package runtimeconfig

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
)

func Read(path string, destination any) error {
	f, err := os.Open(path)
	if err != nil {
		return errors.New("cannot open configuration")
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil || len(b) > 1<<20 {
		return errors.New("configuration exceeds size limit or cannot be read")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(destination); err != nil {
		return errors.New("invalid configuration JSON")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("unexpected configuration data")
	}
	return nil
}
func HexKey(name string, size int) ([]byte, error) {
	key, err := hex.DecodeString(os.Getenv(name))
	if err != nil || len(key) != size {
		return nil, errors.New("missing or invalid key configuration")
	}
	return key, nil
}
