package secrets

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/require"
)

func sampleKey(id string) StoredKey {
	return StoredKey{KeyID: id, Raw: "raw-secret-" + id, Meta: json.RawMessage(`{"role":"reader"}`)}
}

func TestValidateKeyID(t *testing.T) {
	require.NoError(t, ValidateKeyID("agent_0-writer.1"))
	for _, bad := range []string{"", "../escape", "a/b", "a..b", strings.Repeat("x", 200)} {
		require.ErrorIs(t, ValidateKeyID(bad), ErrInvalidKeyID, bad)
	}
}

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	keys, err := s.LoadAll(ctx)
	require.NoError(t, err)
	require.Empty(t, keys)

	require.NoError(t, s.Put(ctx, sampleKey("k1")))
	require.NoError(t, s.Put(ctx, sampleKey("k2")))
	keys, err = s.LoadAll(ctx)
	require.NoError(t, err)
	require.Len(t, keys, 2)

	require.NoError(t, s.Delete(ctx, "k1"))
	keys, err = s.LoadAll(ctx)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, "k2", keys[0].KeyID)
	require.ErrorIs(t, s.Put(ctx, StoredKey{KeyID: "../bad"}), ErrInvalidKeyID)
}

func TestFileStore(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "keys")
	s, err := NewFileStore(dir)
	require.NoError(t, err)
	require.NoError(t, s.Put(ctx, sampleKey("file_key")))

	keys, err := s.LoadAll(ctx)
	require.NoError(t, err)
	require.Equal(t, []StoredKey{sampleKey("file_key")}, keys)

	require.NoError(t, s.Delete(ctx, "file_key"))
	keys, err = s.LoadAll(ctx)
	require.NoError(t, err)
	require.Empty(t, keys)
	require.ErrorIs(t, s.Put(ctx, StoredKey{KeyID: "../escape"}), ErrInvalidKeyID)
}

func TestNewFactory(t *testing.T) {
	ctx := context.Background()
	_, err := New(ctx, Config{Provider: "memory"})
	require.NoError(t, err)
	_, err = New(ctx, Config{Provider: "file", Path: filepath.Join(t.TempDir(), "keys")})
	require.NoError(t, err)
	_, err = New(ctx, Config{Provider: "bogus"})
	require.Error(t, err)
}

func TestVaultStore(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	stored := sampleKey("vault_key")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, "test-token", r.Header.Get("X-Vault-Token"))
		switch r.Method {
		case "LIST":
			_, _ = io.WriteString(w, `{"data":{"keys":["vault_key"]}}`)
		case http.MethodGet:
			require.Equal(t, "/v1/secret/data/longbow/rbac/vault_key", r.URL.Path)
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": stored}})
		case http.MethodPut:
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			data, ok := body["data"].(map[string]any)
			require.True(t, ok)
			encoded, err := json.Marshal(data)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(encoded, &stored))
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	s, err := NewVaultStore(VaultConfig{Addr: srv.URL, Token: "test-token"})
	require.NoError(t, err)
	require.NoError(t, s.Put(ctx, sampleKey("vault_key")))
	keys, err := s.LoadAll(ctx)
	require.NoError(t, err)
	require.Equal(t, []StoredKey{stored}, keys)
	require.NoError(t, s.Delete(ctx, "vault_key"))
}

func TestGCPStore(t *testing.T) {
	ctx := context.Background()
	const secretID = "longbow-rbac-gcp_key"
	payload := base64.StdEncoding.EncodeToString(mustJSON(t, sampleKey("gcp_key")))

	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":addVersion"):
			var body struct {
				Payload struct {
					Data string `json:"data"`
				} `json:"payload"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, payload, body.Payload.Data)
			_, _ = io.WriteString(w, `{}`)
		case r.Method == http.MethodPost:
			require.Equal(t, secretID, r.URL.Query().Get("secretId"))
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{}`)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, ":access"):
			_, _ = io.WriteString(w, fmt.Sprintf(`{"payload":{"data":%q}}`, payload))
		case r.Method == http.MethodGet:
			_, _ = io.WriteString(w, fmt.Sprintf(`{"secrets":[{"name":"projects/p/secrets/%s"}]}`, secretID))
		case r.Method == http.MethodDelete:
			_, _ = io.WriteString(w, `{}`)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	s, err := NewGCPStore(ctx, GCPConfig{
		ProjectID: "p",
		Token:     "test-token",
		BaseURL:   srv.URL + "/v1/projects/p/secrets",
	})
	require.NoError(t, err)
	require.NoError(t, s.Put(ctx, sampleKey("gcp_key")))
	keys, err := s.LoadAll(ctx)
	require.NoError(t, err)
	require.Equal(t, []StoredKey{sampleKey("gcp_key")}, keys)
	require.NoError(t, s.Delete(ctx, "gcp_key"))
}

func TestAWSStore(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	created := false
	secretString := string(mustJSON(t, sampleKey("aws_key")))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		require.Contains(t, r.Header.Get("X-Amz-Target"), "secretsmanager.")
		require.NotEmpty(t, r.Header.Get("X-Amz-Date"))
		switch r.Header.Get("X-Amz-Target") {
		case "secretsmanager.CreateSecret":
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, "longbow/rbac/aws_key", body["Name"])
			if created {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"__type":"ResourceExistsException","message":"exists"}`)
				return
			}
			created = true
			_, _ = io.WriteString(w, `{}`)
		case "secretsmanager.PutSecretValue":
			_, _ = io.WriteString(w, `{}`)
		case "secretsmanager.GetSecretValue":
			_, _ = io.WriteString(w, fmt.Sprintf(`{"SecretString":%q}`, secretString))
		case "secretsmanager.ListSecrets":
			_, _ = io.WriteString(w, `{"SecretList":[{"Name":"longbow/rbac/aws_key"}]}`)
		case "secretsmanager.DeleteSecret":
			_, _ = io.WriteString(w, `{}`)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	s, err := NewAWSStore(ctx, AWSConfig{
		Region:      "us-east-1",
		Endpoint:    srv.URL,
		Credentials: credentials.NewStaticCredentialsProvider("AKID", "SECRET", ""),
	})
	require.NoError(t, err)
	require.NoError(t, s.Put(ctx, sampleKey("aws_key")))
	// Second Put exercises the CreateSecret->PutSecretValue rotation path.
	require.NoError(t, s.Put(ctx, sampleKey("aws_key")))
	keys, err := s.LoadAll(ctx)
	require.NoError(t, err)
	require.Equal(t, []StoredKey{sampleKey("aws_key")}, keys)
	require.NoError(t, s.Delete(ctx, "aws_key"))
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	return data
}
