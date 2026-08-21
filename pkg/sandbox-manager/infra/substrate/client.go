/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package substrate implements the Sandbox backend on top of Substrate actors.
//
// Substrate splits a sandbox into two resources that this package keeps apart:
// an ActorTemplate is an immutable build artifact produced by the E2B template
// build API, while a WorkerPool is capacity declared by a SandboxSet. An actor
// is one instance derived from a template and placed on some pool's worker.
package substrate

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	grpcinsecure "google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// InsecureSchemePrefix marks an address that must be dialed without TLS.
const InsecureSchemePrefix = "insecure://"

// Client wraps the Substrate control-plane gRPC connection.
type Client struct {
	conn    *grpc.ClientConn
	control ateapipb.ControlClient
}

// NewClient dials the Substrate control API. An address prefixed with
// "insecure://" is dialed in plaintext; every other address uses TLS.
//
// caFile names a PEM bundle used to verify the server certificate. It is
// required for TLS addresses: without a trust anchor the connection would have
// to skip verification, which would let anything on the network impersonate the
// control plane that governs every actor's lifecycle.
//
// tokenFile names a file holding the bearer token presented on every RPC. The
// Substrate API authenticates callers by Kubernetes ServiceAccount JWT, so an
// unauthenticated connection is rejected before it reaches a handler. Empty
// dials without a token, which only works against a server that has
// authentication disabled.
func NewClient(addr, caFile, tokenFile string) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("substrate address must not be empty")
	}

	opts := make([]grpc.DialOption, 0, 2)
	switch {
	case strings.HasPrefix(addr, InsecureSchemePrefix):
		addr = strings.TrimPrefix(addr, InsecureSchemePrefix)
		opts = append(opts, grpc.WithTransportCredentials(grpcinsecure.NewCredentials()))
	default:
		tlsCreds, err := loadTLSCredentials(caFile)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(tlsCreds))
	}
	if tokenFile != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(fileBearerToken(tokenFile)))
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial substrate control at %s: %w", addr, err)
	}
	return &Client{conn: conn, control: ateapipb.NewControlClient(conn)}, nil
}

// fileBearerToken presents the token stored in a file as gRPC call credentials.
//
// The file is read per call rather than cached because a projected
// ServiceAccount token is rewritten in place as it approaches expiry; a cached
// value would keep working until the token expired and then fail for good.
type fileBearerToken string

func (f fileBearerToken) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	raw, err := os.ReadFile(string(f))
	if err != nil {
		return nil, fmt.Errorf("read substrate token file %s: %w", string(f), err)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return nil, fmt.Errorf("substrate token file %s is empty", string(f))
	}
	return map[string]string{"authorization": "Bearer " + token}, nil
}

// RequireTransportSecurity reports false so an "insecure://" address stays
// dialable in development; production addresses carry TLS from the dial options.
func (f fileBearerToken) RequireTransportSecurity() bool { return false }

func loadTLSCredentials(caFile string) (credentials.TransportCredentials, error) {
	if caFile == "" {
		return nil, fmt.Errorf("a CA bundle is required to dial substrate over TLS; " +
			"pass the CA file or prefix the address with " + InsecureSchemePrefix +
			" to dial in plaintext")
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read substrate CA bundle %s: %w", caFile, err)
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("substrate CA bundle %s contains no valid PEM certificates", caFile)
	}
	return credentials.NewTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}), nil
}

// Control returns the control-plane client.
func (c *Client) Control() ateapipb.ControlClient {
	if c == nil {
		return nil
	}
	return c.control
}

// Close releases the underlying connection.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// actorRef builds the (atespace, name) reference every actor RPC takes.
func actorRef(atespace, actorID string) *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: atespace, Name: actorID}
}

// EnsureAtespace creates the atespace backing a namespace if it does not exist.
// Substrate scopes actors by atespace, so a sandbox namespace must have one
// before its first actor is created. An already existing atespace is success.
func EnsureAtespace(ctx context.Context, control ateapipb.ControlClient, atespace string) error {
	if control == nil {
		return fmt.Errorf("substrate control client is not configured")
	}
	_, err := control.GetAtespace(ctx, &ateapipb.GetAtespaceRequest{
		Atespace: &ateapipb.ObjectRef{Name: atespace},
	})
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("get atespace %s: %w", atespace, err)
	}

	_, err = control.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{
			Metadata: &ateapipb.ResourceMetadata{Name: atespace},
		},
	})
	// A concurrent manager replica may have won the race; that is still success.
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create atespace %s: %w", atespace, err)
	}
	return nil
}
