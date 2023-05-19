package aws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go-v2/aws"
	signerv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"go.uber.org/multierr"
)

const (
	signService      = "kafka-cluster"
	signVersion      = "2020_10_22"
	signAction       = "kafka-cluster:Connect"
	signActionKey    = "action"
	signHostKey      = "host"
	signUserAgentKey = "user-agent"
	signVersionKey   = "version"
	queryActionKey   = "Action"
	queryExpiryKey   = "X-Amz-Expires"

	emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	defaultExpiry = 5 * time.Minute
)

const (
	_ int32 = iota // Ignoring the zero value to ensure we start up correctly
	initMessage
	serverResponse
	complete
	failed
)

// Errors.
var (
	ErrFailedServerChallenge = errors.New("failed server challenge")
	ErrBadChallenge          = errors.New("invalid challenge data provided")
	ErrInvalidStateReached   = errors.New("invalid state reached")
)

// Client represents a client for authentication with AWS MSK using IAM.
type Client struct {
	// The signerv4.signer of aws-sdk-go-v2 to use when signing the request.
	signer *signerv4.Signer
	// The aws.Config.credentials or config.CredentialsProvider of
	// aws-sdk-go-v2.
	credentials aws.CredentialsProvider

	// The region where the msk cluster is hosted, e.g. "us-east-1".
	region string

	// The duration for which the presigned request is active.
	// Defaults to 5 minutes.
	expiry time.Duration

	// userAgent is the user agent to for the client to use when connecting
	// to Kafka, overriding the default "franz-go/<runtime.Version()>/<hostname>".
	//
	// Setting a userAgent allows authorizing based on the aws:userAgent
	// condition key; see the following link for more details:
	//
	//     https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html#condition-keys-useragent
	//
	userAgent string

	state int32

	// now returns the current local time. It can be override for testing.
	now func() time.Time
}

type response struct {
	Version   string `json:"version"`
	RequestID string `json:"request-id"`
}

var _ sarama.SCRAMClientWithContext = (*Client)(nil)

// NewClient creates and returns a new instance of Client.
func NewClient(
	credentials aws.CredentialsProvider, region string,
	expiry time.Duration, userAgent string,
) *Client {
	if expiry <= 0 {
		expiry = defaultExpiry
	}

	return &Client{
		signer:      signerv4.NewSigner(),
		credentials: credentials,
		region:      region,
		expiry:      expiry,
		userAgent:   userAgent,
		now:         time.Now,
	}
}

func (c *Client) Begin(ctx context.Context, username, password, authzID string) error {
	if c.credentials == nil {
		return errors.New("missing required credentials provider")
	}
	if c.region == "" {
		return errors.New("missing AWS region")
	}

	c.state = initMessage
	return nil
}

func (c *Client) Step(ctx context.Context, challenge string) (string, error) {
	var resp string

	switch c.state {
	case initMessage:
		if challenge != "" {
			c.state = failed
			return "", fmt.Errorf("challenge must be empty for initial request: %w", ErrBadChallenge)
		}
		payload, err := c.getAuthPayload(ctx)
		if err != nil {
			c.state = failed
			return "", err
		}
		resp = string(payload)
		c.state = serverResponse
	case serverResponse:
		if challenge == "" {
			c.state = failed
			return "", fmt.Errorf("challenge must not be empty for server resposne: %w", ErrBadChallenge)
		}

		var resp response
		if err := json.NewDecoder(strings.NewReader(challenge)).Decode(&resp); err != nil {
			c.state = failed
			return "", fmt.Errorf("unable to process msk challenge response: %w", multierr.Combine(err, ErrFailedServerChallenge))
		}

		if resp.Version != signVersion {
			c.state = failed
			return "", fmt.Errorf("unknown version found in response: %w", ErrFailedServerChallenge)
		}

		c.state = complete
	default:
		return "", fmt.Errorf("invalid invocation: %w", ErrInvalidStateReached)
	}

	return resp, nil
}

// Done should return true when the SCRAM conversation is over.
func (c *Client) Done(ctx context.Context) bool { return c.state == complete }

func (c *Client) getAuthPayload(ctx context.Context) ([]byte, error) {
	md := sarama.SASLMetadataFromContext(ctx)
	if md == nil {
		return nil, errors.New("missing sasl metadata")
	}

	req, err := http.NewRequest(http.MethodGet, "kafka://"+md.Host, nil)
	if err != nil {
		return nil, err
	}

	expiry := strconv.Itoa(int(c.expiry.Seconds()))
	query := req.URL.Query()
	query.Set(queryActionKey, signAction)
	query.Set(queryExpiryKey, expiry)
	req.URL.RawQuery = query.Encode()

	creds, err := c.credentials.Retrieve(ctx)
	if err != nil {
		return nil, err
	}

	signedUrl, header, err := c.signer.PresignHTTP(
		ctx, creds, req, emptyPayloadHash, signService, c.region, c.now(),
	)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(signedUrl)
	if err != nil {
		return nil, err
	}

	signedMap := map[string]string{
		signVersionKey:   signVersion,
		signHostKey:      u.Host,
		signUserAgentKey: c.userAgent,
	}
	// The protocol requires lowercase keys.
	for key, vals := range header {
		signedMap[strings.ToLower(key)] = vals[0]
	}
	for key, vals := range u.Query() {
		signedMap[strings.ToLower(key)] = vals[0]
	}

	return json.Marshal(signedMap)
}
