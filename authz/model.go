// Package authz ships the versioned authorization model for deployment by the host.
package authz

import _ "embed"

// ModelJSON is generated from model.fga by generate.py. Deploy it explicitly and pin the returned model ID.
//
//go:embed model.json
var ModelJSON []byte
