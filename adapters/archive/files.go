// Package archive supplies encrypted local archive objects and chain verification.
// Files are create-once through this API. Production hosts must provide a
// separately administered immutable mount or replace this development adapter;
// ordinary files alone do not resist a privileged operator or storage rollback.
package archive

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"io"
	"os"
	"path/filepath"
)

type Cipher interface {
	Seal(string, []byte) ([]byte, error)
	Open(string, []byte) ([]byte, error)
}
type Files struct {
	path   string
	cipher Cipher
}

func NewFiles(path string, cipher Cipher) (*Files, error) {
	if cipher == nil || !filepath.IsAbs(path) {
		return nil, model.ErrInvalid
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, model.ErrInvalid
	}
	return &Files{path, cipher}, nil
}
func objectKey(tenant, key string) string {
	return model.Digest([]string{"archive-object-v1", tenant, key})
}
func digest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func (f *Files) read(ctx context.Context, tenant, locator string) (model.ObjectRef, []byte, error) {
	if err := ctx.Err(); err != nil {
		return model.ObjectRef{}, nil, err
	}
	if !model.ValidID(tenant) || !digest(locator) {
		return model.ObjectRef{}, nil, model.ErrInvalid
	}
	file, err := os.Open(filepath.Join(f.path, locator))
	if errors.Is(err, os.ErrNotExist) {
		return model.ObjectRef{}, nil, model.ErrNotFound
	}
	if err != nil {
		return model.ObjectRef{}, nil, err
	}
	defer file.Close()
	b, err := io.ReadAll(io.LimitReader(file, (64<<20)+1024))
	if err != nil {
		return model.ObjectRef{}, nil, err
	}
	if len(b) >= 64<<20+1024 {
		return model.ObjectRef{}, nil, model.ErrPrecondition
	}
	plain, err := f.cipher.Open(tenant, b)
	if err != nil {
		return model.ObjectRef{}, nil, model.ErrPrecondition
	}
	ref := model.ObjectRef{Locator: locator, Version: model.BytesDigest(b), Digest: model.BytesDigest(plain), Size: int64(len(plain))}
	return ref, plain, nil
}
func (f *Files) Load(ctx context.Context, tenant, key string) (model.ObjectRef, []byte, error) {
	return f.read(ctx, tenant, objectKey(tenant, key))
}
func (f *Files) Read(ctx context.Context, tenant string, ref model.ObjectRef) ([]byte, error) {
	actual, b, err := f.read(ctx, tenant, ref.Locator)
	if err != nil {
		return nil, err
	}
	if actual != ref {
		return nil, model.ErrPrecondition
	}
	return b, nil
}
func (f *Files) Put(ctx context.Context, tenant, key string, plain []byte) (model.ObjectRef, error) {
	if !model.ValidID(tenant) || key == "" || len(key) > 1024 || len(plain) > 64<<20 {
		return model.ObjectRef{}, model.ErrInvalid
	}
	existing, old, err := f.Load(ctx, tenant, key)
	if err == nil {
		if !bytes.Equal(old, plain) {
			return model.ObjectRef{}, model.ErrConflict
		}
		if err := f.syncDirectory(); err != nil {
			return model.ObjectRef{}, err
		}
		return existing, nil
	}
	if !errors.Is(err, model.ErrNotFound) {
		return model.ObjectRef{}, err
	}
	b, err := f.cipher.Seal(tenant, plain)
	if err != nil {
		return model.ObjectRef{}, err
	}
	temp, err := os.CreateTemp(f.path, ".pending-")
	if err != nil {
		return model.ObjectRef{}, err
	}
	defer os.Remove(temp.Name())
	defer temp.Close()
	if _, err = temp.Write(b); err != nil {
		return model.ObjectRef{}, err
	}
	if err = temp.Sync(); err != nil {
		return model.ObjectRef{}, err
	}
	if err = temp.Close(); err != nil {
		return model.ObjectRef{}, err
	}
	if err = ctx.Err(); err != nil {
		return model.ObjectRef{}, err
	}
	// Hard-link publication is atomic and never replaces an existing object.
	err = os.Link(temp.Name(), filepath.Join(f.path, objectKey(tenant, key)))
	if err != nil && !errors.Is(err, os.ErrExist) {
		return model.ObjectRef{}, err
	}
	if err = f.syncDirectory(); err != nil {
		return model.ObjectRef{}, err
	}
	existing, old, err = f.Load(ctx, tenant, key)
	if err != nil {
		return model.ObjectRef{}, err
	}
	if !bytes.Equal(old, plain) {
		return model.ObjectRef{}, model.ErrConflict
	}
	return existing, nil
}

func (f *Files) syncDirectory() error {
	dir, err := os.Open(f.path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
