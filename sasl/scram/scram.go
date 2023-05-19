package scram

import (
	"context"
	"errors"

	"github.com/Shopify/sarama"
	"github.com/xdg-go/scram"
)

// Client represents a client for authentication with Kafka using SCRAM.
type Client struct {
	client             *scram.Client
	clientConversation *scram.ClientConversation
	hashGeneratorFcn   scram.HashGeneratorFcn
}

var _ sarama.SCRAMClientWithContext = (*Client)(nil)

// NewClient creates and returns a new instance of Client.
func NewClient(hashGeneratorFcn scram.HashGeneratorFcn) *Client {
	return &Client{hashGeneratorFcn: hashGeneratorFcn}
}

// Begin prepares the client for the SCRAM exchange with the server with a
// username and a password.
func (c *Client) Begin(_ context.Context, username, password, authzID string) (err error) {
	if c.hashGeneratorFcn == nil {
		return errors.New("missing required hash generator")
	}

	c.client, err = c.hashGeneratorFcn.NewClient(username, password, authzID)
	if err != nil {
		return err
	}
	c.clientConversation = c.client.NewConversation()
	return nil
}

// Step steps client through the SCRAM exchange. It is called repeatedly until
// it errors or `Done` returns true.
func (c *Client) Step(_ context.Context, challenge string) (response string, err error) {
	response, err = c.clientConversation.Step(challenge)
	return
}

// Done should return true when the SCRAM conversation is over.
func (c *Client) Done(_ context.Context) bool {
	return c.clientConversation.Done()
}
