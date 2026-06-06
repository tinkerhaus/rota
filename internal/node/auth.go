package node

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/fsm"
	"github.com/tinkerhaus/rota/internal/storage"
)

const BootstrapAdminPrincipal = "root"

type AuthCheck struct {
	Action rotav1.AuthAction
	Lane   string
	Group  string
}

type AuthDecision struct {
	Enabled   bool
	Allowed   bool
	Known     bool
	Principal string
	Reason    string
}

func (n *Node) AuthEnabled() (bool, error) {
	lo, hi := storage.AuthPrincipalBounds()
	it, err := n.store.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return false, err
	}
	defer it.Close()
	return it.First(), nil
}

func (n *Node) BootstrapAuthAdmin(name, token string) error {
	if strings.TrimSpace(token) == "" {
		return nil
	}
	if name = strings.TrimSpace(name); name == "" {
		name = BootstrapAdminPrincipal
	}
	enabled, err := n.AuthEnabled()
	if err != nil || enabled || !n.IsLeader() {
		return err
	}
	_, _, err = n.createAuthPrincipalWithToken(name, []string{"administrator"}, []*rotav1.AuthGrant{{
		LanePattern: ".*", GroupPattern: ".*", Actions: []rotav1.AuthAction{rotav1.AuthAction_AUTH_ADMIN},
		Note: "bootstrap administrator",
	}}, token)
	return err
}

func (n *Node) CreateAuthPrincipal(name string, tags []string, grants []*rotav1.AuthGrant) (*rotav1.AuthPrincipal, string, error) {
	token, err := mintAuthToken()
	if err != nil {
		return nil, "", err
	}
	return n.createAuthPrincipalWithToken(name, tags, grants, token)
}

func (n *Node) createAuthPrincipalWithToken(name string, tags []string, grants []*rotav1.AuthGrant, token string) (*rotav1.AuthPrincipal, string, error) {
	name, tags, grants, err := validatePrincipalInput(name, tags, grants)
	if err != nil {
		return nil, "", err
	}
	tok, err := authTokenMaterial(token)
	if err != nil {
		return nil, "", err
	}
	res, err := n.apply(fsm.Command{Type: fsm.CmdAuthPrincipal, AuthPrincipal: &fsm.AuthPrincipalCmd{
		Op:        fsm.AuthCreatePrincipal,
		Name:      name,
		Tags:      tags,
		Grants:    grantsToCmd(grants),
		TokenID:   tok.TokenId,
		Salt:      tok.Salt,
		TokenHash: tok.Hash,
		NowMs:     nowMs(),
	}})
	if err != nil {
		return nil, "", err
	}
	p, _ := res.(*rotav1.AuthPrincipal)
	return redactPrincipal(p), token, nil
}

func (n *Node) RotateAuthPrincipalToken(name string) (*rotav1.AuthPrincipal, string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, "", fmt.Errorf("principal name is required")
	}
	token, err := mintAuthToken()
	if err != nil {
		return nil, "", err
	}
	tok, err := authTokenMaterial(token)
	if err != nil {
		return nil, "", err
	}
	res, err := n.apply(fsm.Command{Type: fsm.CmdAuthPrincipal, AuthPrincipal: &fsm.AuthPrincipalCmd{
		Op:        fsm.AuthRotatePrincipalToken,
		Name:      name,
		TokenID:   tok.TokenId,
		Salt:      tok.Salt,
		TokenHash: tok.Hash,
		NowMs:     nowMs(),
	}})
	if err != nil {
		return nil, "", err
	}
	p, _ := res.(*rotav1.AuthPrincipal)
	return redactPrincipal(p), token, nil
}

func (n *Node) SetAuthPrincipalDisabled(name string, disabled bool) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("principal name is required")
	}
	_, err := n.apply(fsm.Command{Type: fsm.CmdAuthPrincipal, AuthPrincipal: &fsm.AuthPrincipalCmd{
		Op:       fsm.AuthSetPrincipalDisabled,
		Name:     name,
		Disabled: disabled,
		NowMs:    nowMs(),
	}})
	return err
}

func (n *Node) GrantAuthPrincipal(name string, grant *rotav1.AuthGrant) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("principal name is required")
	}
	grants, err := validateGrants([]*rotav1.AuthGrant{grant})
	if err != nil {
		return err
	}
	_, err = n.apply(fsm.Command{Type: fsm.CmdAuthPrincipal, AuthPrincipal: &fsm.AuthPrincipalCmd{
		Op:    fsm.AuthGrantPrincipal,
		Name:  name,
		Grant: grantToCmd(grants[0]),
		NowMs: nowMs(),
	}})
	return err
}

func (n *Node) RevokeAuthPrincipalGrant(name string, grantIndex uint32) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("principal name is required")
	}
	_, err := n.apply(fsm.Command{Type: fsm.CmdAuthPrincipal, AuthPrincipal: &fsm.AuthPrincipalCmd{
		Op:         fsm.AuthRevokePrincipalGrant,
		Name:       name,
		GrantIndex: grantIndex,
		NowMs:      nowMs(),
	}})
	return err
}

func (n *Node) ListAuthPrincipals() ([]*rotav1.AuthPrincipal, error) {
	return n.listAuthPrincipals(true)
}

func (n *Node) listAuthPrincipals(redact bool) ([]*rotav1.AuthPrincipal, error) {
	lo, hi := storage.AuthPrincipalBounds()
	it, err := n.store.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []*rotav1.AuthPrincipal
	for it.First(); it.Valid(); it.Next() {
		p := &rotav1.AuthPrincipal{}
		if proto.Unmarshal(it.Value(), p) != nil {
			continue
		}
		if redact {
			p = redactPrincipal(p)
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (n *Node) AuthorizeToken(token string, checks []AuthCheck) (AuthDecision, error) {
	enabled, err := n.AuthEnabled()
	if err != nil || !enabled {
		return AuthDecision{Enabled: enabled, Allowed: !enabled}, err
	}
	if strings.TrimSpace(token) == "" {
		return AuthDecision{Enabled: true, Reason: "missing token"}, nil
	}
	principals, err := n.listAuthPrincipals(false)
	if err != nil {
		return AuthDecision{Enabled: true}, err
	}
	for _, p := range principals {
		if !principalTokenMatches(p, token) {
			continue
		}
		decision := AuthDecision{Enabled: true, Known: true, Principal: p.GetName()}
		if p.GetDisabled() {
			decision.Reason = "principal disabled"
			return decision, nil
		}
		if len(checks) == 0 {
			checks = []AuthCheck{{Action: rotav1.AuthAction_AUTH_ADMIN}}
		}
		for _, check := range checks {
			if !principalAllows(p, check) {
				decision.Reason = fmt.Sprintf("%s not allowed on lane=%q group=%q", check.Action.String(), check.Lane, check.Group)
				return decision, nil
			}
		}
		decision.Allowed = true
		return decision, nil
	}
	return AuthDecision{Enabled: true, Reason: "unknown token"}, nil
}

func validatePrincipalInput(name string, tags []string, grants []*rotav1.AuthGrant) (string, []string, []*rotav1.AuthGrant, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", nil, nil, fmt.Errorf("principal name is required")
	}
	if strings.ContainsAny(name, "\x00\r\n\t ") {
		return "", nil, nil, fmt.Errorf("principal name may not contain whitespace or control characters")
	}
	grants, err := validateGrants(grants)
	if err != nil {
		return "", nil, nil, err
	}
	return name, canonicalTags(tags), grants, nil
}

func validateGrants(grants []*rotav1.AuthGrant) ([]*rotav1.AuthGrant, error) {
	out := make([]*rotav1.AuthGrant, 0, len(grants))
	for _, g := range grants {
		if g == nil {
			return nil, fmt.Errorf("grant is required")
		}
		cp := proto.Clone(g).(*rotav1.AuthGrant)
		cp.LanePattern = normalizePattern(cp.GetLanePattern())
		cp.GroupPattern = normalizePattern(cp.GetGroupPattern())
		if _, err := regexp.Compile(cp.LanePattern); err != nil {
			return nil, fmt.Errorf("invalid lane pattern %q: %w", cp.LanePattern, err)
		}
		if _, err := regexp.Compile(cp.GroupPattern); err != nil {
			return nil, fmt.Errorf("invalid group pattern %q: %w", cp.GroupPattern, err)
		}
		if len(cp.Actions) == 0 {
			return nil, fmt.Errorf("grant must include at least one action")
		}
		seen := map[rotav1.AuthAction]bool{}
		actions := cp.Actions[:0]
		for _, a := range cp.Actions {
			if a == rotav1.AuthAction_AUTH_ACTION_UNSPECIFIED {
				return nil, fmt.Errorf("grant includes unspecified action")
			}
			if !seen[a] {
				seen[a] = true
				actions = append(actions, a)
			}
		}
		cp.Actions = actions
		out = append(out, cp)
	}
	return out, nil
}

func canonicalTags(tags []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, tag := range tags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag == "" || seen[tag] {
			continue
		}
		seen[tag] = true
		out = append(out, tag)
	}
	sort.Strings(out)
	return out
}

func grantsToCmd(grants []*rotav1.AuthGrant) []fsm.AuthGrantCmd {
	out := make([]fsm.AuthGrantCmd, 0, len(grants))
	for _, g := range grants {
		out = append(out, *grantToCmd(g))
	}
	return out
}

func grantToCmd(g *rotav1.AuthGrant) *fsm.AuthGrantCmd {
	actions := make([]int32, 0, len(g.GetActions()))
	for _, a := range g.GetActions() {
		actions = append(actions, int32(a))
	}
	return &fsm.AuthGrantCmd{
		LanePattern:  g.GetLanePattern(),
		GroupPattern: g.GetGroupPattern(),
		Actions:      actions,
		Note:         g.GetNote(),
	}
}

func principalAllows(p *rotav1.AuthPrincipal, check AuthCheck) bool {
	for _, tag := range p.GetTags() {
		switch strings.ToLower(tag) {
		case "administrator":
			return true
		case "dashboard", "monitoring":
			if check.Action == rotav1.AuthAction_AUTH_READ {
				return true
			}
		}
	}
	for _, grant := range p.GetGrants() {
		if grantAllows(grant, check) {
			return true
		}
	}
	return false
}

func grantAllows(grant *rotav1.AuthGrant, check AuthCheck) bool {
	if grant == nil || !grantHasAction(grant, check.Action) {
		return false
	}
	lanePattern := normalizePattern(grant.GetLanePattern())
	groupPattern := normalizePattern(grant.GetGroupPattern())
	laneOK, _ := regexp.MatchString(lanePattern, check.Lane)
	groupOK, _ := regexp.MatchString(groupPattern, check.Group)
	return laneOK && groupOK
}

func grantHasAction(grant *rotav1.AuthGrant, action rotav1.AuthAction) bool {
	for _, a := range grant.GetActions() {
		if a == rotav1.AuthAction_AUTH_ADMIN || a == action {
			return true
		}
	}
	return false
}

func normalizePattern(pattern string) string {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" || pattern == "*" {
		return ".*"
	}
	return pattern
}

func principalTokenMatches(p *rotav1.AuthPrincipal, token string) bool {
	info := p.GetToken()
	if info == nil || len(info.GetSalt()) == 0 || len(info.GetHash()) == 0 {
		return false
	}
	h := saltedTokenHash(info.GetSalt(), token)
	return subtle.ConstantTimeCompare(h, info.GetHash()) == 1
}

func mintAuthToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "rota_" + base64.RawURLEncoding.EncodeToString(raw), nil
}

func authTokenMaterial(token string) (*rotav1.AuthTokenInfo, error) {
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("token is required")
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	idHash := sha256.Sum256([]byte(token))
	return &rotav1.AuthTokenInfo{
		TokenId: hex.EncodeToString(idHash[:8]),
		Salt:    salt,
		Hash:    saltedTokenHash(salt, token),
	}, nil
}

func saltedTokenHash(salt []byte, token string) []byte {
	h := sha256.New()
	_, _ = h.Write(salt)
	_, _ = h.Write([]byte(token))
	return h.Sum(nil)
}

func redactPrincipal(p *rotav1.AuthPrincipal) *rotav1.AuthPrincipal {
	if p == nil {
		return nil
	}
	cp := proto.Clone(p).(*rotav1.AuthPrincipal)
	if cp.Token != nil {
		cp.Token.Salt = nil
		cp.Token.Hash = nil
	}
	return cp
}
