package workerdaemon

import (
	"errors"
	"io"
	"os"
	"regexp"

	"github.com/caxqueiroz/facts_engine/adapters/extractionhttp"
	"github.com/caxqueiroz/facts_engine/internal/runtimeconfig"
	"github.com/caxqueiroz/facts_engine/model"
)

type ExtractionGateway struct {
	PropagateTraceContext  bool   `json:"propagate_trace_context,omitempty"`
	ProfileID              string `json:"profile_id"`
	URL                    string `json:"url"`
	TokenEnv               string `json:"token_env,omitempty"`
	RootCAFile             string `json:"root_ca_file,omitempty"`
	AllowPlaintextLoopback bool   `json:"allow_plaintext_loopback,omitempty"`
}
type ExtractionConfig struct {
	Tenant     string                              `json:"tenant"`
	Stream     string                              `json:"stream"`
	Scheduling *runtimeconfig.ExtractionScheduling `json:"scheduling"`
	Gateways   []ExtractionGateway                 `json:"gateways"`
}

var gatewayEnvName = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)

func (c *ExtractionConfig) Validate() error {
	if c == nil || !model.ValidID(c.Tenant) || !model.ValidID(c.Stream) || c.Scheduling.Validate() != nil || len(c.Gateways) != len(c.Scheduling.Profiles) {
		return model.ErrInvalid
	}
	profiles := map[string]bool{}
	for _, p := range c.Scheduling.Profiles {
		if !model.ValidID(p.ID) || profiles[p.ID] {
			return model.ErrInvalid
		}
		profiles[p.ID] = true
	}
	seen := map[string]bool{}
	for _, g := range c.Gateways {
		if !profiles[g.ProfileID] || seen[g.ProfileID] || g.URL == "" || (g.TokenEnv != "" && !gatewayEnvName.MatchString(g.TokenEnv)) {
			return model.ErrInvalid
		}
		seen[g.ProfileID] = true
	}
	return nil
}
func openExtractionGateways(c *ExtractionConfig) (model.ExtractionOptions, func(), error) {
	options := c.Scheduling.Options()
	options.ScheduleOnly = false
	providers := []*extractionhttp.Provider{}
	closeAll := func() {
		for _, p := range providers {
			p.Close()
		}
	}
	bindings := map[string]ExtractionGateway{}
	for _, g := range c.Gateways {
		bindings[g.ProfileID] = g
	}
	for i, approved := range options.Profiles {
		g := bindings[approved.Profile.ID]
		var roots []byte
		if g.RootCAFile != "" {
			f, err := os.Open(g.RootCAFile)
			if err != nil {
				closeAll()
				return options, closeAll, errors.New("cannot open gateway CA")
			}
			roots, err = io.ReadAll(io.LimitReader(f, (1<<20)+1))
			_ = f.Close()
			if err != nil || len(roots) == 0 || len(roots) > 1<<20 {
				closeAll()
				return options, closeAll, errors.New("invalid gateway CA size")
			}
		}
		token := ""
		if g.TokenEnv != "" {
			token = os.Getenv(g.TokenEnv)
			if token == "" {
				closeAll()
				return options, closeAll, errors.New("missing gateway credential")
			}
		}
		p, err := extractionhttp.New(extractionhttp.Config{PropagateTraceContext: g.PropagateTraceContext, URL: g.URL, Token: token, RootCAPEM: roots, AllowPlaintextLoopback: g.AllowPlaintextLoopback, Timeout: approved.Profile.Timeout, MaxInputBytes: approved.Profile.MaxInputBytes, MaxOutputBytes: approved.Profile.MaxOutputBytes})
		if err != nil {
			closeAll()
			return options, closeAll, errors.New("invalid extraction gateway configuration")
		}
		providers = append(providers, p)
		options.Profiles[i].Provider = p
	}
	return options, closeAll, nil
}
