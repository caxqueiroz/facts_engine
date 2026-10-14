package daemon

import (
	"encoding/json"
	"github.com/caxqueiroz/facts_engine/adapters/mtlsidentity"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStrictConfig(t *testing.T) {
	valid := Config{Listen: "127.0.0.1:8443", TLSCertificate: "server.pem", TLSKey: "server.key", ClientCA: "ca.pem", Identities: []mtlsidentity.Binding{{URI: "spiffe://test/worker", Tenant: "tenant", Principal: "worker"}}, OpenFGAURL: "http://localhost:8080", OpenFGAStore: "store", OpenFGAModel: "model", ReadinessScope: "scope", MaxConnections: 4, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20, RPCTimeoutSeconds: 10, ShutdownSeconds: 10}
	b, _ := json.Marshal(valid)
	tests := []struct {
		name, data string
		ok         bool
	}{{"valid", string(b), true}, {"unknown field", strings.TrimSuffix(string(b), "}") + `,"trust_identity_headers":true}`, false}, {"trailing JSON", string(b) + `{}`, false}, {"oversize", string(b) + strings.Repeat(" ", 1<<20), false}, {"empty", `{}`, false}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(test.data), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadConfig(path)
			if (err == nil) != test.ok {
				t.Fatalf("config error: %v", err)
			}
		})
	}
	valid.RPCTimeoutSeconds = 121
	if valid.Validate() == nil {
		t.Fatal("unbounded RPC timeout")
	}
}

func TestExtractionSchedulingExample(t *testing.T) {
	c, err := LoadConfig("../../examples/memoryd-extraction.json")
	if err != nil {
		t.Fatal(err)
	}
	if c.Extraction == nil || !c.Extraction.Options().ScheduleOnly {
		t.Fatal("example lacks provider-free scheduling")
	}
	c.Extraction.Profiles = nil
	if c.Validate() == nil {
		t.Fatal("empty extraction configuration accepted")
	}
}
