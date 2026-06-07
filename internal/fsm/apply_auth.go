package fsm

import (
	"fmt"

	"github.com/cockroachdb/pebble"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/storage"
)

func (f *FSM) applyAuthPrincipal(b *pebble.Batch, c *AuthPrincipalCmd) (interface{}, error) {
	if c == nil {
		return nil, fmt.Errorf("auth principal command is nil")
	}
	if c.Name == "" {
		return nil, fmt.Errorf("principal name is required")
	}
	switch c.Op {
	case AuthCreatePrincipal:
		return f.applyAuthCreatePrincipal(b, c)
	case AuthRotatePrincipalToken:
		return f.applyAuthRotatePrincipalToken(b, c)
	case AuthSetPrincipalDisabled:
		return f.applyAuthSetPrincipalDisabled(b, c)
	case AuthGrantPrincipal:
		return f.applyAuthGrantPrincipal(b, c)
	case AuthRevokePrincipalGrant:
		return f.applyAuthRevokePrincipalGrant(b, c)
	default:
		return nil, fmt.Errorf("unknown auth principal op %d", c.Op)
	}
}

func (f *FSM) applyAuthCreatePrincipal(b *pebble.Batch, c *AuthPrincipalCmd) (interface{}, error) {
	if _, ok, err := f.s.GetRaw(storage.AuthPrincipalKey(c.Name)); err != nil {
		return nil, err
	} else if ok {
		return nil, fmt.Errorf("principal %q already exists", c.Name)
	}
	if c.TokenID == "" || len(c.Salt) == 0 || len(c.TokenHash) == 0 {
		return nil, fmt.Errorf("principal token material is required")
	}
	p := &rotav1.AuthPrincipal{
		Name:      c.Name,
		Tags:      append([]string(nil), c.Tags...),
		Grants:    authGrantsFromCmd(c.Grants),
		CreatedMs: c.NowMs,
		UpdatedMs: c.NowMs,
		Token: &rotav1.AuthTokenInfo{
			TokenId:   c.TokenID,
			Salt:      append([]byte(nil), c.Salt...),
			Hash:      append([]byte(nil), c.TokenHash...),
			CreatedMs: c.NowMs,
			RotatedMs: c.NowMs,
		},
	}
	if err := putProto(b, storage.AuthPrincipalKey(c.Name), p); err != nil {
		return nil, err
	}
	return p, nil
}

func (f *FSM) applyAuthRotatePrincipalToken(b *pebble.Batch, c *AuthPrincipalCmd) (interface{}, error) {
	p, err := f.loadAuthPrincipal(c.Name)
	if err != nil {
		return nil, err
	}
	if c.TokenID == "" || len(c.Salt) == 0 || len(c.TokenHash) == 0 {
		return nil, fmt.Errorf("principal token material is required")
	}
	created := c.NowMs
	if p.GetToken() != nil && p.GetToken().GetCreatedMs() != 0 {
		created = p.GetToken().GetCreatedMs()
	}
	p.Token = &rotav1.AuthTokenInfo{
		TokenId:   c.TokenID,
		Salt:      append([]byte(nil), c.Salt...),
		Hash:      append([]byte(nil), c.TokenHash...),
		CreatedMs: created,
		RotatedMs: c.NowMs,
	}
	p.UpdatedMs = c.NowMs
	if err := putProto(b, storage.AuthPrincipalKey(c.Name), p); err != nil {
		return nil, err
	}
	return p, nil
}

func (f *FSM) applyAuthSetPrincipalDisabled(b *pebble.Batch, c *AuthPrincipalCmd) (interface{}, error) {
	p, err := f.loadAuthPrincipal(c.Name)
	if err != nil {
		return nil, err
	}
	p.Disabled = c.Disabled
	p.UpdatedMs = c.NowMs
	if err := putProto(b, storage.AuthPrincipalKey(c.Name), p); err != nil {
		return nil, err
	}
	return &rotav1.AuthOpResult{Ok: true}, nil
}

func (f *FSM) applyAuthGrantPrincipal(b *pebble.Batch, c *AuthPrincipalCmd) (interface{}, error) {
	p, err := f.loadAuthPrincipal(c.Name)
	if err != nil {
		return nil, err
	}
	if c.Grant == nil {
		return nil, fmt.Errorf("grant is required")
	}
	p.Grants = append(p.Grants, authGrantFromCmd(*c.Grant))
	p.UpdatedMs = c.NowMs
	if err := putProto(b, storage.AuthPrincipalKey(c.Name), p); err != nil {
		return nil, err
	}
	return &rotav1.AuthOpResult{Ok: true}, nil
}

func (f *FSM) applyAuthRevokePrincipalGrant(b *pebble.Batch, c *AuthPrincipalCmd) (interface{}, error) {
	p, err := f.loadAuthPrincipal(c.Name)
	if err != nil {
		return nil, err
	}
	if int(c.GrantIndex) >= len(p.Grants) {
		return nil, fmt.Errorf("grant index %d out of range", c.GrantIndex)
	}
	p.Grants = append(p.Grants[:c.GrantIndex], p.Grants[c.GrantIndex+1:]...)
	p.UpdatedMs = c.NowMs
	if err := putProto(b, storage.AuthPrincipalKey(c.Name), p); err != nil {
		return nil, err
	}
	return &rotav1.AuthOpResult{Ok: true}, nil
}

func (f *FSM) loadAuthPrincipal(name string) (*rotav1.AuthPrincipal, error) {
	p := &rotav1.AuthPrincipal{}
	found, err := f.s.GetProto(storage.AuthPrincipalKey(name), p)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("principal %q not found", name)
	}
	return p, nil
}

func authGrantsFromCmd(in []AuthGrantCmd) []*rotav1.AuthGrant {
	out := make([]*rotav1.AuthGrant, 0, len(in))
	for _, g := range in {
		out = append(out, authGrantFromCmd(g))
	}
	return out
}

func authGrantFromCmd(g AuthGrantCmd) *rotav1.AuthGrant {
	actions := make([]rotav1.AuthAction, 0, len(g.Actions))
	for _, a := range g.Actions {
		actions = append(actions, rotav1.AuthAction(a))
	}
	return &rotav1.AuthGrant{
		LanePattern:  g.LanePattern,
		GroupPattern: g.GroupPattern,
		Actions:      actions,
		Note:         g.Note,
	}
}
