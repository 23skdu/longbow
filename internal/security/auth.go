package security

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/github"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/microsoft"
)

var (
	ErrInvalidToken      = errors.New("invalid token")
	ErrTokenExpired      = errors.New("token expired")
	ErrInvalidProvider   = errors.New("invalid OAuth provider")
	ErrMissingCode       = errors.New("authorization code missing")
	ErrUserNotAuthorized = errors.New("user not authorized")
)

type OAuthProvider string

const (
	ProviderGitHub    OAuthProvider = "github"
	ProviderGoogle    OAuthProvider = "google"
	ProviderMicrosoft OAuthProvider = "microsoft"
	ProviderOIDC      OAuthProvider = "oidc"
)

type SSOConfig struct {
	Enabled         bool            `json:"enabled"`
	Providers       []OAuthProvider `json:"providers"`
	ClientID        string          `json:"client_id"`
	ClientSecret    string          `json:"client_secret"`
	RedirectURL     string          `json:"redirect_url"`
	Scopes          []string        `json:"scopes"`
	AllowedDomains  []string        `json:"allowed_domains"`
	SessionLifetime time.Duration   `json:"session_lifetime"`
	JWKSPort        int             `json:"jwks_port"`
}

type OAuthState struct {
	State    string
	Provider OAuthProvider
	Expiry   time.Time
}

type JWTClaims struct {
	Sub      string   `json:"sub"`
	Iss      string   `json:"iss"`
	Aud      string   `json:"aud"`
	Exp      int64    `json:"exp"`
	Iat      int64    `json:"iat"`
	Nbf      int64    `json:"nbf"`
	Name     string   `json:"name"`
	Email    string   `json:"email"`
	Groups   []string `json:"groups"`
	Provider string   `json:"provider"`
}

type SSOToken struct {
	AccessToken  string
	RefreshToken string
	Expiry       time.Time
	Claims       *JWTClaims
}

type OAuthManager struct {
	mu           sync.RWMutex
	config       SSOConfig
	oauth2Config map[OAuthProvider]*oauth2.Config
	states       map[string]OAuthState
	tokens       map[string]*SSOToken
	sessionStore *SessionStore
	key          *rsa.PrivateKey
}

type SessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

type Session struct {
	ID        string
	UserID    string
	Email     string
	Name      string
	Provider  string
	CreatedAt time.Time
	ExpiresAt time.Time
	Data      map[string]interface{}
}

func NewOAuthManager(config SSOConfig) (*OAuthManager, error) {
	if config.ClientID == "" || config.ClientSecret == "" {
		return nil, errors.New("OAuth client ID and secret are required")
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate RSA key: %w", err)
	}

	mgr := &OAuthManager{
		config:       config,
		oauth2Config: make(map[OAuthProvider]*oauth2.Config),
		states:       make(map[string]OAuthState),
		tokens:       make(map[string]*SSOToken),
		sessionStore: NewSessionStore(),
		key:          key,
	}

	for _, provider := range config.Providers {
		mgr.oauth2Config[provider] = mgr.createOAuth2Config(provider)
	}

	return mgr, nil
}

func (m *OAuthManager) createOAuth2Config(provider OAuthProvider) *oauth2.Config {
	baseCfg := &oauth2.Config{
		ClientID:     m.config.ClientID,
		ClientSecret: m.config.ClientSecret,
		RedirectURL:  m.config.RedirectURL,
		Scopes:       m.config.Scopes,
	}

	switch provider {
	case ProviderGitHub:
		baseCfg.Endpoint = github.Endpoint
	case ProviderGoogle:
		baseCfg.Endpoint = google.Endpoint
	case ProviderMicrosoft:
		baseCfg.Endpoint = microsoft.AzureADEndpoint("common")
	case ProviderOIDC:
		baseCfg.Endpoint = oauth2.Endpoint{ // #nosec G101 - official OIDC endpoint URLs, not credentials
			AuthURL:  "https://accounts.google.com/o/oauth2/auth",
			TokenURL: "https://oauth2.googleapis.com/token",
		}
	}

	return baseCfg
}

func (m *OAuthManager) GetAuthURL(provider OAuthProvider, state string) (string, error) {
	cfg, ok := m.oauth2Config[provider]
	if !ok {
		return "", ErrInvalidProvider
	}

	m.mu.Lock()
	m.states[state] = OAuthState{
		State:    state,
		Provider: provider,
		Expiry:   time.Now().Add(10 * time.Minute),
	}
	m.mu.Unlock()

	return cfg.AuthCodeURL(state, oauth2.AccessTypeOffline), nil
}

func (m *OAuthManager) ExchangeCode(ctx context.Context, provider OAuthProvider, code string) (*SSOToken, error) {
	cfg, ok := m.oauth2Config[provider]
	if !ok {
		return nil, ErrInvalidProvider
	}

	token, err := cfg.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("code exchange failed: %w", err)
	}

	ssotoken := &SSOToken{
		AccessToken:  token.AccessToken,
		RefreshToken: token.RefreshToken,
		Expiry:       token.Expiry,
	}

	return ssotoken, nil
}

func (m *OAuthManager) ValidateToken(ctx context.Context, tokenString string) (*JWTClaims, error) {
	m.mu.RLock()
	token := m.tokens[tokenString]
	m.mu.RUnlock()

	if token == nil {
		return nil, ErrInvalidToken
	}

	if time.Now().After(token.Expiry) {
		return nil, ErrTokenExpired
	}

	return token.Claims, nil
}

func (m *OAuthManager) CreateSession(token *SSOToken, userInfo UserInfo) (*Session, error) {
	session := &Session{
		ID:        generateSessionID(),
		UserID:    userInfo.ID,
		Email:     userInfo.Email,
		Name:      userInfo.Name,
		Provider:  userInfo.Provider,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(m.config.SessionLifetime),
		Data:      make(map[string]interface{}),
	}

	m.sessionStore.Store(session)
	return session, nil
}

func (m *OAuthManager) GetSession(sessionID string) (*Session, error) {
	return m.sessionStore.Get(sessionID)
}

func (m *OAuthManager) DeleteSession(sessionID string) {
	m.sessionStore.Delete(sessionID)
}

func (m *OAuthManager) RefreshToken(ctx context.Context, refreshToken string) (*SSOToken, error) {
	m.mu.RLock()
	var cfg *oauth2.Config
	for _, c := range m.oauth2Config {
		cfg = c
		break
	}
	m.mu.RUnlock()

	if cfg == nil {
		return nil, ErrInvalidProvider
	}

	tok := &oauth2.Token{RefreshToken: refreshToken}
	newToken, err := cfg.TokenSource(ctx, tok).Token()
	if err != nil {
		return nil, err
	}

	return &SSOToken{
		AccessToken:  newToken.AccessToken,
		RefreshToken: newToken.RefreshToken,
		Expiry:       newToken.Expiry,
	}, nil
}

func NewSessionStore() *SessionStore {
	store := &SessionStore{
		sessions: make(map[string]*Session),
	}
	go store.cleanupExpired()
	return store
}

func (s *SessionStore) Store(session *Session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[session.ID] = session
}

func (s *SessionStore) Get(id string) (*Session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, ok := s.sessions[id]
	if !ok {
		return nil, errors.New("session not found")
	}
	if time.Now().After(session.ExpiresAt) {
		return nil, errors.New("session expired")
	}
	return session, nil
}

func (s *SessionStore) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, id)
}

func (s *SessionStore) cleanupExpired() {
	ticker := time.NewTicker(5 * time.Minute)
	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		for id, session := range s.sessions {
			if now.After(session.ExpiresAt) {
				delete(s.sessions, id)
			}
		}
		s.mu.Unlock()
	}
}

type UserInfo struct {
	ID       string
	Name     string
	Email    string
	Avatar   string
	Provider string
}

func (m *OAuthManager) GetUserInfo(ctx context.Context, provider OAuthProvider, token *SSOToken) (*UserInfo, error) {
	switch provider {
	case ProviderGitHub:
		return m.getGitHubUserInfo(ctx, token)
	case ProviderGoogle:
		return m.getGoogleUserInfo(ctx, token)
	case ProviderMicrosoft:
		return m.getMicrosoftUserInfo(ctx, token)
	default:
		return nil, ErrInvalidProvider
	}
}

func (m *OAuthManager) getGitHubUserInfo(ctx context.Context, token *SSOToken) (*UserInfo, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://api.github.com/user", nil)
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var user struct {
		ID        int    `json:"id"`
		Login     string `json:"login"`
		Name      string `json:"name"`
		Email     string `json:"email"`
		AvatarURL string `json:"avatar_url"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return nil, err
	}

	return &UserInfo{
		ID:       fmt.Sprintf("github:%d", user.ID),
		Name:     user.Name,
		Email:    user.Email,
		Avatar:   user.AvatarURL,
		Provider: string(ProviderGitHub),
	}, nil
}

func (m *OAuthManager) getGoogleUserInfo(ctx context.Context, token *SSOToken) (*UserInfo, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://www.googleapis.com/oauth2/v2/userinfo", nil)
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var user struct {
		ID            string `json:"id"`
		Name          string `json:"name"`
		Email         string `json:"email"`
		Picture       string `json:"picture"`
		VerifiedEmail bool   `json:"verified_email"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return nil, err
	}

	return &UserInfo{
		ID:       "google:" + user.ID,
		Name:     user.Name,
		Email:    user.Email,
		Avatar:   user.Picture,
		Provider: string(ProviderGoogle),
	}, nil
}

func (m *OAuthManager) getMicrosoftUserInfo(ctx context.Context, token *SSOToken) (*UserInfo, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://graph.microsoft.com/v1.0/me", nil)
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var user struct {
		ID                string `json:"id"`
		DisplayName       string `json:"displayName"`
		Mail              string `json:"mail"`
		UserPrincipalName string `json:"userPrincipalName"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return nil, err
	}

	email := user.Mail
	if email == "" {
		email = user.UserPrincipalName
	}

	return &UserInfo{
		ID:       "microsoft:" + user.ID,
		Name:     user.DisplayName,
		Email:    email,
		Provider: string(ProviderMicrosoft),
	}, nil
}

func generateSessionID() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}

func (m *OAuthManager) Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !m.config.Enabled {
				next.ServeHTTP(w, r)
				return
			}

			sessionID := r.Header.Get("X-Session-ID")
			if sessionID == "" {
				cookie, err := r.Cookie("session")
				if err == nil {
					sessionID = cookie.Value
				}
			}

			if sessionID != "" {
				session, err := m.GetSession(sessionID)
				if err == nil {
					ctx := context.WithValue(r.Context(), "session", session)
					r = r.WithContext(ctx)
				}
			}

			next.ServeHTTP(w, r)
		})
	}
}

type Claims struct {
	Sub   string `json:"sub"`
	Email string `json:"email"`
	Name  string `json:"name"`
	Admin bool   `json:"admin"`
	Iss   string `json:"iss"`
	Aud   string `json:"aud"`
	Exp   int64  `json:"exp"`
	Iat   int64  `json:"iat"`
	Nbf   int64  `json:"nbf"`
}

func (c *Claims) Valid() error {
	now := time.Now().Unix()
	if c.Exp < now {
		return ErrTokenExpired
	}
	if c.Nbf > now {
		return errors.New("token not yet valid")
	}
	return nil
}

type TokenGenerator struct {
	privateKey *rsa.PrivateKey
	issuer     string
	audience   string
}

func NewTokenGenerator(issuer, audience string) (*TokenGenerator, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	return &TokenGenerator{
		privateKey: key,
		issuer:     issuer,
		audience:   audience,
	}, nil
}

func (g *TokenGenerator) GenerateToken(claims Claims) (string, error) {
	claims.Iss = g.issuer
	claims.Aud = g.audience
	claims.Iat = time.Now().Unix()
	claims.Exp = time.Now().Add(time.Hour).Unix()
	claims.Nbf = claims.Iat

	tokenString, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	encoded := base64.RawURLEncoding.EncodeToString(tokenString)
	return encoded, nil
}

func (g *TokenGenerator) ValidateToken(tokenString string) (*Claims, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(tokenString)
	if err != nil {
		return nil, err
	}

	var claims Claims
	if err := json.Unmarshal(decoded, &claims); err != nil {
		return nil, err
	}

	if err := claims.Valid(); err != nil {
		return nil, err
	}

	return &claims, nil
}

func (m *OAuthManager) IsDomainAllowed(email string) bool {
	if len(m.config.AllowedDomains) == 0 {
		return true
	}

	domain := strings.Split(email, "@")
	if len(domain) < 2 {
		return false
	}

	for _, allowed := range m.config.AllowedDomains {
		if domain[1] == allowed {
			return true
		}
	}

	return false
}

func (m *OAuthManager) RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session, ok := r.Context().Value("session").(*Session)
		if !ok || session == nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (m *OAuthManager) RequireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session, ok := r.Context().Value("session").(*Session)
		if !ok || session == nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		admin, ok := session.Data["admin"].(bool)
		if !ok || !admin {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	})
}
