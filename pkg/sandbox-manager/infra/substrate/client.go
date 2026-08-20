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
func NewClient(addr, caFile string) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("substrate address must not be empty")
	}

	var creds grpc.DialOption
	switch {
	case strings.HasPrefix(addr, InsecureSchemePrefix):
		addr = strings.TrimPrefix(addr, InsecureSchemePrefix)
		creds = grpc.WithTransportCredentials(grpcinsecure.NewCredentials())
	default:
		tlsCreds, err := loadTLSCredentials(caFile)
		if err != nil {
			return nil, err
		}
		creds = grpc.WithTransportCredentials(tlsCreds)
	}

	conn, err := grpc.NewClient(addr, creds)
	if err != nil {
		return nil, fmt.Errorf("dial substrate control at %s: %w", addr, err)
	}
	return &Client{conn: conn, control: ateapipb.NewControlClient(conn)}, nil
}

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
