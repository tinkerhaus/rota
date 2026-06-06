package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
)

func cmdAuth(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: rota auth <list|create|rotate|enable|disable|grant|revoke> [flags]")
	}
	switch args[0] {
	case "list":
		return cmdAuthList(args[1:])
	case "create":
		return cmdAuthCreate(args[1:])
	case "rotate":
		return cmdAuthRotate(args[1:])
	case "enable":
		return cmdAuthSetDisabled(args[1:], false)
	case "disable":
		return cmdAuthSetDisabled(args[1:], true)
	case "grant":
		return cmdAuthGrant(args[1:])
	case "revoke":
		return cmdAuthRevoke(args[1:])
	default:
		return fmt.Errorf("unknown auth command %q", args[0])
	}
}

func cmdAuthList(args []string) error {
	var opts clientOptions
	var asJSON bool
	fs := flag.NewFlagSet("auth list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts)
	fs.BoolVar(&asJSON, "json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return withClients(opts, func(ctx context.Context, clients *rotaClients) error {
		resp, err := clients.control.ListPrincipals(ctx, &rotav1.ListPrincipalsRequest{})
		if err != nil {
			return err
		}
		return writeAuthList(os.Stdout, resp, asJSON)
	})
}

func cmdAuthCreate(args []string) error {
	var opts clientOptions
	var name, lane, group, actions, note string
	var tags, grantSpecs multiFlag
	var asJSON bool
	fs := flag.NewFlagSet("auth create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts)
	fs.StringVar(&name, "name", "", "principal name")
	fs.Var(&tags, "tag", "principal tag; repeatable (administrator, dashboard, monitoring)")
	fs.Var(&grantSpecs, "grant", "grant as lane_regex:group_regex:actions; repeatable")
	fs.StringVar(&lane, "lane", "", "lane regex for a single grant")
	fs.StringVar(&group, "group", "", "group regex for a single grant")
	fs.StringVar(&actions, "actions", "", "comma-separated actions for a single grant")
	fs.StringVar(&note, "note", "", "note for the single grant")
	fs.BoolVar(&asJSON, "json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireFlag("name", name); err != nil {
		return err
	}
	grants, err := buildAuthGrants(grantSpecs, lane, group, actions, note)
	if err != nil {
		return err
	}
	return withClients(opts, func(ctx context.Context, clients *rotaClients) error {
		resp, err := clients.control.CreatePrincipal(ctx, &rotav1.CreatePrincipalRequest{Name: name, Tags: tags, Grants: grants})
		if err != nil {
			return err
		}
		return writeAuthCreate(os.Stdout, resp.GetPrincipal(), resp.GetToken(), asJSON)
	})
}

func cmdAuthRotate(args []string) error {
	var opts clientOptions
	var name string
	var asJSON bool
	fs := flag.NewFlagSet("auth rotate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts)
	fs.StringVar(&name, "name", "", "principal name")
	fs.BoolVar(&asJSON, "json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireFlag("name", name); err != nil {
		return err
	}
	return withClients(opts, func(ctx context.Context, clients *rotaClients) error {
		resp, err := clients.control.RotatePrincipalToken(ctx, &rotav1.RotatePrincipalTokenRequest{Name: name})
		if err != nil {
			return err
		}
		return writeAuthCreate(os.Stdout, resp.GetPrincipal(), resp.GetToken(), asJSON)
	})
}

func cmdAuthSetDisabled(args []string, disabled bool) error {
	var opts clientOptions
	var name string
	fs := flag.NewFlagSet("auth set-disabled", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts)
	fs.StringVar(&name, "name", "", "principal name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireFlag("name", name); err != nil {
		return err
	}
	return withClients(opts, func(ctx context.Context, clients *rotaClients) error {
		_, err := clients.control.SetPrincipalDisabled(ctx, &rotav1.SetPrincipalDisabledRequest{Name: name, Disabled: disabled})
		if err == nil {
			state := "enabled"
			if disabled {
				state = "disabled"
			}
			fmt.Fprintf(os.Stdout, "principal %s %s\n", name, state)
		}
		return err
	})
}

func cmdAuthGrant(args []string) error {
	var opts clientOptions
	var name, lane, group, actions, note string
	var grantSpecs multiFlag
	fs := flag.NewFlagSet("auth grant", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts)
	fs.StringVar(&name, "name", "", "principal name")
	fs.Var(&grantSpecs, "grant", "grant as lane_regex:group_regex:actions; repeatable")
	fs.StringVar(&lane, "lane", "", "lane regex")
	fs.StringVar(&group, "group", "", "group regex")
	fs.StringVar(&actions, "actions", "", "comma-separated actions")
	fs.StringVar(&note, "note", "", "grant note")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireFlag("name", name); err != nil {
		return err
	}
	grants, err := buildAuthGrants(grantSpecs, lane, group, actions, note)
	if err != nil {
		return err
	}
	if len(grants) != 1 {
		return fmt.Errorf("auth grant requires exactly one grant")
	}
	return withClients(opts, func(ctx context.Context, clients *rotaClients) error {
		_, err := clients.control.GrantPrincipal(ctx, &rotav1.GrantPrincipalRequest{Name: name, Grant: grants[0]})
		if err == nil {
			fmt.Fprintf(os.Stdout, "grant added principal=%s\n", name)
		}
		return err
	})
}

func cmdAuthRevoke(args []string) error {
	var opts clientOptions
	var name string
	var index uint
	fs := flag.NewFlagSet("auth revoke", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts)
	fs.StringVar(&name, "name", "", "principal name")
	fs.UintVar(&index, "grant-index", 0, "grant index from auth list")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireFlag("name", name); err != nil {
		return err
	}
	return withClients(opts, func(ctx context.Context, clients *rotaClients) error {
		_, err := clients.control.RevokePrincipalGrant(ctx, &rotav1.RevokePrincipalGrantRequest{Name: name, GrantIndex: uint32(index)})
		if err == nil {
			fmt.Fprintf(os.Stdout, "grant revoked principal=%s index=%d\n", name, index)
		}
		return err
	})
}

type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

func buildAuthGrants(specs []string, lane, group, actions, note string) ([]*rotav1.AuthGrant, error) {
	var grants []*rotav1.AuthGrant
	for _, spec := range specs {
		g, err := parseGrantSpec(spec)
		if err != nil {
			return nil, err
		}
		grants = append(grants, g)
	}
	if actions != "" || lane != "" || group != "" || note != "" {
		parsed, err := parseAuthActions(actions)
		if err != nil {
			return nil, err
		}
		grants = append(grants, &rotav1.AuthGrant{LanePattern: lane, GroupPattern: group, Actions: parsed, Note: note})
	}
	return grants, nil
}

func parseGrantSpec(spec string) (*rotav1.AuthGrant, error) {
	parts := strings.SplitN(spec, ":", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("grant %q must be lane_regex:group_regex:actions", spec)
	}
	actions, err := parseAuthActions(parts[2])
	if err != nil {
		return nil, err
	}
	return &rotav1.AuthGrant{LanePattern: parts[0], GroupPattern: parts[1], Actions: actions}, nil
}

func parseAuthActions(s string) ([]rotav1.AuthAction, error) {
	var out []rotav1.AuthAction
	for _, raw := range strings.Split(s, ",") {
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "":
			continue
		case "all":
			out = append(out,
				rotav1.AuthAction_AUTH_READ,
				rotav1.AuthAction_AUTH_PUBLISH,
				rotav1.AuthAction_AUTH_CONSUME,
				rotav1.AuthAction_AUTH_COMPLETE,
				rotav1.AuthAction_AUTH_WORKFLOW,
				rotav1.AuthAction_AUTH_CONFIGURE,
			)
		case "read":
			out = append(out, rotav1.AuthAction_AUTH_READ)
		case "publish", "write":
			out = append(out, rotav1.AuthAction_AUTH_PUBLISH)
		case "consume", "work":
			out = append(out, rotav1.AuthAction_AUTH_CONSUME)
		case "complete":
			out = append(out, rotav1.AuthAction_AUTH_COMPLETE)
		case "workflow":
			out = append(out, rotav1.AuthAction_AUTH_WORKFLOW)
		case "configure", "config":
			out = append(out, rotav1.AuthAction_AUTH_CONFIGURE)
		case "admin":
			out = append(out, rotav1.AuthAction_AUTH_ADMIN)
		default:
			return nil, fmt.Errorf("unknown auth action %q", raw)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one auth action is required")
	}
	return dedupeActions(out), nil
}

func dedupeActions(in []rotav1.AuthAction) []rotav1.AuthAction {
	seen := map[rotav1.AuthAction]bool{}
	out := in[:0]
	for _, a := range in {
		if !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	return out
}

func writeAuthList(w io.Writer, resp *rotav1.ListPrincipalsResponse, asJSON bool) error {
	if asJSON {
		return json.NewEncoder(w).Encode(resp)
	}
	fmt.Fprintf(w, "auth enabled=%t principals=%d\n", resp.GetAuthEnabled(), len(resp.GetPrincipals()))
	for _, p := range resp.GetPrincipals() {
		tokenID := ""
		if p.GetToken() != nil {
			tokenID = p.GetToken().GetTokenId()
		}
		fmt.Fprintf(w, "\n%s disabled=%t tags=%s token_id=%s\n", p.GetName(), p.GetDisabled(), strings.Join(p.GetTags(), ","), tokenID)
		for i, g := range p.GetGrants() {
			fmt.Fprintf(w, "  [%d] lane=%q group=%q actions=%s", i, g.GetLanePattern(), g.GetGroupPattern(), actionNames(g.GetActions()))
			if g.GetNote() != "" {
				fmt.Fprintf(w, " note=%q", g.GetNote())
			}
			fmt.Fprintln(w)
		}
	}
	return nil
}

func writeAuthCreate(w io.Writer, p *rotav1.AuthPrincipal, token string, asJSON bool) error {
	if asJSON {
		return json.NewEncoder(w).Encode(struct {
			Principal *rotav1.AuthPrincipal `json:"principal"`
			Token     string                `json:"token"`
		}{Principal: p, Token: token})
	}
	fmt.Fprintf(w, "principal=%s token=%s\n", p.GetName(), token)
	fmt.Fprintln(w, "store this token now; it is not shown again")
	return nil
}

func actionNames(actions []rotav1.AuthAction) string {
	names := make([]string, 0, len(actions))
	for _, a := range actions {
		name := strings.TrimPrefix(a.String(), "AUTH_")
		names = append(names, strings.ToLower(name))
	}
	return strings.Join(names, ",")
}

func parseUint32Flag(name, value string) (uint32, error) {
	v, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("--%s must be a uint32", name)
	}
	return uint32(v), nil
}
