package secrets

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const gcpScope = "https://www.googleapis.com/auth/cloud-platform"

// GCPConfig configures Google Secret Manager storage. Credentials use the
// standard application-default chain unless an explicit token is supplied.
type GCPConfig struct {
	ProjectID   string
	Token       string
	Prefix      string
	BaseURL     string // optional override for tests / private endpoints
	TokenSource oauth2.TokenSource
	Client      *http.Client
}

// GCPStore stores each RBAC credential as a Secret Manager secret whose
// payload is the JSON-encoded StoredKey.
type GCPStore struct {
	projectID string
	prefix    string
	baseURL   string
	client    *http.Client
}

var _ Store = (*GCPStore)(nil)

func gcpSecretID(prefix, keyID string) (string, error) {
	if err := ValidateKeyID(keyID); err != nil {
		return "", err
	}
	clean := strings.NewReplacer("/", "-", ".", "-", "_", "-").Replace(strings.ToLower(prefix))
	return clean + "-" + strings.ToLower(keyID), nil
}

func NewGCPStore(ctx context.Context, cfg GCPConfig) (*GCPStore, error) {
	if strings.TrimSpace(cfg.ProjectID) == "" {
		return nil, fmt.Errorf("secrets: gcp project id must not be empty")
	}
	if strings.TrimSpace(cfg.Prefix) == "" {
		cfg.Prefix = DefaultPrefix
	}
	ts := cfg.TokenSource
	if ts == nil {
		if cfg.Token != "" {
			ts = oauth2.StaticTokenSource(&oauth2.Token{AccessToken: cfg.Token})
		} else {
			creds, err := google.FindDefaultCredentials(ctx, gcpScope)
			if err != nil {
				return nil, fmt.Errorf("secrets: gcp credentials: %w", err)
			}
			ts = creds.TokenSource
		}
	}
	client := cfg.Client
	if client == nil {
		client = oauth2.NewClient(ctx, ts)
	}
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = "https://secretmanager.googleapis.com/v1/projects/" + url.PathEscape(cfg.ProjectID) + "/secrets"
	}
	return &GCPStore{
		projectID: cfg.ProjectID,
		prefix:    strings.NewReplacer("/", "-", ".", "-", "_", "-").Replace(strings.ToLower(strings.Trim(cfg.Prefix, "/"))),
		baseURL:   strings.TrimRight(baseURL, "/"),
		client:    client,
	}, nil
}

func (s *GCPStore) do(ctx context.Context, method, path string, body, out any) (int, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, path, reader)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("secrets: gcp request: %w", err)
	}
	defer resp.Body.Close() // #nosec G104
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("secrets: gcp %s: %s: %s", method, resp.Status, strings.TrimSpace(string(payload)))
	}
	if out != nil && len(payload) > 0 {
		if err := json.Unmarshal(payload, out); err != nil {
			return resp.StatusCode, fmt.Errorf("secrets: decode gcp response: %w", err)
		}
	}
	return resp.StatusCode, nil
}

func (s *GCPStore) LoadAll(ctx context.Context) ([]StoredKey, error) {
	var out []StoredKey
	page := ""
	for {
		u := s.baseURL + "?pageSize=100"
		if page != "" {
			u += "&pageToken=" + url.QueryEscape(page)
		}
		var listed struct {
			Secrets []struct {
				Name string `json:"name"`
			} `json:"secrets"`
			NextPageToken string `json:"nextPageToken"`
		}
		if _, err := s.do(ctx, http.MethodGet, u, nil, &listed); err != nil {
			return nil, err
		}
		for _, secret := range listed.Secrets {
			if !strings.Contains(secret.Name, "/secrets/"+s.prefix+"-") {
				continue
			}
			var got struct {
				Payload struct {
					Data string `json:"data"`
				} `json:"payload"`
			}
			// ListSecrets returns a fully qualified resource name such as
			// "projects/p/secrets/id". baseURL already contains the project
			// path, so append only the secret ID to avoid duplicating it.
			secretID := secret.Name
			if idx := strings.LastIndex(secretID, "/"); idx >= 0 {
				secretID = secretID[idx+1:]
			}
			accessURL := s.baseURL + "/" + url.PathEscape(secretID) + "/versions/latest:access"
			if _, err := s.do(ctx, http.MethodGet, accessURL, nil, &got); err != nil {
				return nil, err
			}
			data, err := base64.StdEncoding.DecodeString(got.Payload.Data)
			if err != nil {
				return nil, fmt.Errorf("secrets: decode gcp payload for %s: %w", secret.Name, err)
			}
			var key StoredKey
			if err := json.Unmarshal(data, &key); err != nil {
				return nil, fmt.Errorf("secrets: decode stored key for %s: %w", secret.Name, err)
			}
			out = append(out, key)
		}
		if listed.NextPageToken == "" {
			break
		}
		page = listed.NextPageToken
	}
	return out, nil
}

func (s *GCPStore) Put(ctx context.Context, key StoredKey) error {
	secretID, err := gcpSecretID(s.prefix, key.KeyID)
	if err != nil {
		return err
	}
	// Create is idempotent: an existing secret is reused and a new version is
	// added below (this is also how rotation is persisted).
	status, err := s.do(ctx, http.MethodPost, s.baseURL+"?secretId="+url.QueryEscape(secretID),
		map[string]any{"replication": map[string]any{"automatic": map[string]any{}}}, nil)
	if err != nil && status != 409 {
		return err
	}
	data, err := json.Marshal(key)
	if err != nil {
		return err
	}
	_, err = s.do(ctx, http.MethodPost, s.baseURL+"/"+url.PathEscape(secretID)+":addVersion",
		map[string]any{"payload": map[string]any{"data": base64.StdEncoding.EncodeToString(data)}}, nil)
	return err
}

func (s *GCPStore) Delete(ctx context.Context, keyID string) error {
	secretID, err := gcpSecretID(s.prefix, keyID)
	if err != nil {
		return err
	}
	_, err = s.do(ctx, http.MethodDelete, s.baseURL+"/"+url.PathEscape(secretID), nil, nil)
	return err
}
