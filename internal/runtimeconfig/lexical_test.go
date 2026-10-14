package runtimeconfig

import "testing"

func TestRouteBounds(t *testing.T) {
	route := LexicalRoute{Tenant: "tenant", Generation: "g1", DatabaseEnv: "MEMORY_INDEX_URL", MaxConnections: 4}
	if err := ValidateRoutes([]LexicalRoute{route}); err != nil {
		t.Fatal(err)
	}
	if ValidateRoutes([]LexicalRoute{route, route}) == nil {
		t.Fatal("duplicate route accepted")
	}
	route.DatabaseEnv = "url-with-credentials"
	if ValidateRoutes([]LexicalRoute{route}) == nil {
		t.Fatal("invalid environment name accepted")
	}
	route.DatabaseEnv = "MEMORY_INDEX_URL"
	route.MaxConnections = 33
	if ValidateRoutes([]LexicalRoute{route}) == nil {
		t.Fatal("unbounded pool accepted")
	}
}
