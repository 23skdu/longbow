package secrets

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

// AWSConfig configures AWS Secrets Manager storage. Credentials use the
// standard AWS chain unless an explicit provider is supplied.
type AWSConfig struct {
	Region      string
	Endpoint    string
	Prefix      string
	Credentials aws.CredentialsProvider
	Client      *http.Client
}

type httpStatusError struct {
	Status int
	Body   string
}

// Error runs the error operation.
func (e *httpStatusError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.Status, strings.TrimSpace(e.Body))
}

// AWSStore stores each credential as a Secrets Manager secret named
// <prefix>/<KeyID>, signed with SigV4 using standard AWS credentials.
type AWSStore struct {
	endpoint string
	region   string
	prefix   string
	creds    aws.CredentialsProvider
	client   *http.Client
}

var _ Store = (*AWSStore)(nil)

// NewAWSStore constructs a new AWSStore.
func NewAWSStore(ctx context.Context, cfg AWSConfig) (*AWSStore, error) {
	if strings.TrimSpace(cfg.Region) == "" {
		return nil, fmt.Errorf("secrets: aws region must not be empty")
	}
	if strings.TrimSpace(cfg.Prefix) == "" {
		cfg.Prefix = DefaultPrefix
	}
	if cfg.Credentials == nil {
		awscfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
		if err != nil {
			return nil, fmt.Errorf("secrets: aws credentials: %w", err)
		}
		cfg.Credentials = awscfg.Credentials
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: defaultHTTPTimeout}
	}
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = "https://secretsmanager." + cfg.Region + ".amazonaws.com"
	}
	return &AWSStore{
		endpoint: strings.TrimRight(endpoint, "/"),
		region:   cfg.Region,
		prefix:   strings.Trim(cfg.Prefix, "/"),
		creds:    cfg.Credentials,
		client:   cfg.Client,
	}, nil
}

func (s *AWSStore) secretName(keyID string) (string, error) {
	if err := ValidateKeyID(keyID); err != nil {
		return "", err
	}
	return s.prefix + "/" + keyID, nil
}

func (s *AWSStore) do(ctx context.Context, operation string, body, out any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "secretsmanager."+operation)
	sum := sha256.Sum256(data)
	creds, err := s.creds.Retrieve(ctx)
	if err != nil {
		return fmt.Errorf("secrets: aws credentials: %w", err)
	}
	signer := v4.NewSigner()
	if err := signer.SignHTTP(ctx, creds, req, hex.EncodeToString(sum[:]), "secretsmanager", s.region, time.Now()); err != nil {
		return fmt.Errorf("secrets: aws sign: %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("secrets: aws request: %w", err)
	}
	defer resp.Body.Close() // #nosec G104
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &httpStatusError{Status: resp.StatusCode, Body: string(payload)}
	}
	if out != nil && len(payload) > 0 {
		if err := json.Unmarshal(payload, out); err != nil {
			return fmt.Errorf("secrets: decode aws response: %w", err)
		}
	}
	return nil
}

// LoadAll runs the load all operation.
func (s *AWSStore) LoadAll(ctx context.Context) ([]StoredKey, error) {
	var out []StoredKey
	next := ""
	for {
		req := map[string]any{"MaxResults": 100}
		if next != "" {
			req["NextToken"] = next
		}
		var listed struct {
			SecretList []struct {
				Name string `json:"Name"`
			} `json:"SecretList"`
			NextToken string `json:"NextToken"`
		}
		if err := s.do(ctx, "ListSecrets", req, &listed); err != nil {
			return nil, err
		}
		for _, secret := range listed.SecretList {
			if !strings.HasPrefix(secret.Name, s.prefix+"/") {
				continue
			}
			var got struct {
				SecretString string `json:"SecretString"`
			}
			if err := s.do(ctx, "GetSecretValue", map[string]any{"SecretId": secret.Name}, &got); err != nil {
				return nil, err
			}
			var key StoredKey
			if err := json.Unmarshal([]byte(got.SecretString), &key); err != nil {
				return nil, fmt.Errorf("secrets: decode aws secret %s: %w", secret.Name, err)
			}
			out = append(out, key)
		}
		if listed.NextToken == "" {
			break
		}
		next = listed.NextToken
	}
	return out, nil
}

// Put runs the put operation.
func (s *AWSStore) Put(ctx context.Context, key StoredKey) error {
	name, err := s.secretName(key.KeyID)
	if err != nil {
		return err
	}
	data, err := json.Marshal(key)
	if err != nil {
		return err
	}
	err = s.do(ctx, "CreateSecret", map[string]any{
		"Name": name, "SecretString": string(data),
	}, nil)
	if err == nil {
		return nil
	}
	var statusErr *httpStatusError
	if !errors.As(err, &statusErr) || !strings.Contains(statusErr.Body, "ResourceExistsException") {
		return err
	}
	return s.do(ctx, "PutSecretValue", map[string]any{
		"SecretId": name, "SecretString": string(data),
	}, nil)
}

// Delete runs the delete operation.
func (s *AWSStore) Delete(ctx context.Context, keyID string) error {
	name, err := s.secretName(keyID)
	if err != nil {
		return err
	}
	err = s.do(ctx, "DeleteSecret", map[string]any{
		"SecretId": name, "ForceDeleteWithoutRecovery": true,
	}, nil)
	if err == nil {
		return nil
	}
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) &&
		(statusErr.Status == http.StatusNotFound || strings.Contains(statusErr.Body, "ResourceNotFoundException")) {
		return fmt.Errorf("%w: aws key %q", ErrNotFound, keyID)
	}
	return err
}
