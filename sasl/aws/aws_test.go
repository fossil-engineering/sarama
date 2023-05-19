package aws

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthentication(t *testing.T) {
	t.Parallel()

	const (
		accessKeyID     = "ACCESS_KEY_ID"
		secretAccessKey = "SECRET_ACCESS_KEY"
		sessionToken    = "SESSION_TOKEN"
		brokerHost      = "xxxxxx.xx.kafka.us-east-1.amazonaws.com"
		brokerPort      = "9098"
		userAgent       = "sarama"
		region          = "us-east-1"
		expiry          = 15 * time.Minute
	)

	var (
		credentials = credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken)
		ctx         = sarama.WithSASLMetadata(context.Background(), &sarama.SASLMetadata{Host: brokerHost, Port: brokerPort})
	)

	client := NewClient(credentials, region, expiry, userAgent)
	require.NotNil(t, client, "Must have a valid client")

	assert.NoError(t, client.Begin(ctx, "", "", ""))
	assert.Equal(t, initMessage, client.state, "Must be in the initial state")

	payload, err := client.Step(ctx, "") // Initial Challenge
	assert.NoError(t, err, "Must not error on the initial challenge")
	assert.NotEmpty(t, payload, "Must have a valid payload with data")

	expectedFields := map[string]struct{}{
		"version":             {},
		"host":                {},
		"user-agent":          {},
		"action":              {},
		"x-amz-algorithm":     {},
		"x-amz-credential":    {},
		"x-amz-date":          {},
		"x-amz-signedheaders": {},
		"x-amz-expires":       {},
		"x-amz-signature":     {},
	}

	var request map[string]string
	assert.NoError(t, json.NewDecoder(strings.NewReader(payload)).Decode(&request))

	for k := range expectedFields {
		v, ok := request[k]
		assert.True(t, ok, "Must have the expected field")
		assert.NotEmpty(t, v, "Must have a value for the field")
	}

	_, err = client.Step(ctx, `{"version": "2020_10_22", "request-id": "pine apple sauce"}`)
	assert.NoError(t, err, "Must not error when given valid challenge")
	assert.True(t, client.Done(ctx), "Must have completed auth")
}

func TestValidatingServerResponse(t *testing.T) {
	t.Parallel()
	const (
		accessKeyID     = "ACCESS_KEY_ID"
		secretAccessKey = "SECRET_ACCESS_KEY"
		sessionToken    = "SESSION_TOKEN"
		brokerHost      = "xxxxxx.xx.kafka.us-east-1.amazonaws.com"
		brokerPort      = "9098"
		userAgent       = "sarama"
		region          = "us-east-1"
		expiry          = 15 * time.Minute
	)

	var (
		credentials = credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken)
		ctx         = sarama.WithSASLMetadata(context.Background(), &sarama.SASLMetadata{Host: brokerHost, Port: brokerPort})
	)

	testCases := []struct {
		scenario   string
		challenge  string
		expectErr  error
		expectDone bool
	}{
		{
			scenario:   "Valid challenge payload",
			challenge:  `{"version": "2020_10_22", "request-id": "pine apple sauce"}`,
			expectErr:  nil,
			expectDone: true,
		},
		{
			scenario:   "Empty challenge response returned",
			challenge:  "",
			expectErr:  ErrBadChallenge,
			expectDone: false,
		},
		{
			scenario:   "Challenge sent with unknown field",
			challenge:  `{"error": "unknown data format"}`,
			expectErr:  ErrFailedServerChallenge,
			expectDone: false,
		},
		{
			scenario:   "Invalid version within challenge",
			challenge:  `{"version": "2022_10_22", "request-id": "pizza sauce"}`,
			expectErr:  ErrFailedServerChallenge,
			expectDone: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.scenario, func(t *testing.T) {
			client := NewClient(credentials, region, expiry, userAgent)

			client.state = serverResponse

			payload, err := client.Step(ctx, tc.challenge)

			assert.ErrorIs(t, err, tc.expectErr, "Must match the expected error in scenario")
			assert.Empty(t, payload, "Must return a blank string")
			assert.Equal(t, tc.expectDone, client.Done(ctx), "Must be in the expected state")
		})
	}

	client := NewClient(credentials, region, expiry, userAgent)
	_, err := client.Step(ctx, "")
	assert.ErrorIs(t, err, ErrInvalidStateReached, "Must be an invalid step when not set up correctly")

}
