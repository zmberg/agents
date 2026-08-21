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

package substrate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, []byte("not a certificate"), 0o600))

	tests := []struct {
		name      string
		addr      string
		caFile    string
		tokenFile string
		wantErr   string
	}{
		{
			name:    "an empty address is rejected",
			wantErr: "address must not be empty",
		},
		{
			// Plaintext needs no trust anchor, so it dials without a CA.
			name: "insecure address dials without a CA",
			addr: InsecureSchemePrefix + "127.0.0.1:50051",
		},
		{
			name:      "insecure address accepts a token file",
			addr:      InsecureSchemePrefix + "127.0.0.1:50051",
			tokenFile: "/var/run/secrets/substrate/token",
		},
		{
			// Skipping verification would let anything on the network
			// impersonate the control plane, so a TLS address demands a CA.
			name:    "TLS address without a CA is rejected",
			addr:    "api.ate-system.svc:443",
			wantErr: "CA bundle is required",
		},
		{
			name:    "TLS address with a CA holding no PEM is rejected",
			addr:    "api.ate-system.svc:443",
			caFile:  caFile,
			wantErr: "contains no valid PEM certificates",
		},
		{
			name:    "TLS address with a missing CA file is rejected",
			addr:    "api.ate-system.svc:443",
			caFile:  filepath.Join(t.TempDir(), "absent.pem"),
			wantErr: "read substrate CA bundle",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(tt.addr, tt.caFile, tt.tokenFile)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, client)
			assert.NotNil(t, client.Control())
			assert.NoError(t, client.Close())
		})
	}

	t.Run("a nil client tolerates Control and Close", func(t *testing.T) {
		var client *Client
		assert.Nil(t, client.Control())
		assert.NoError(t, client.Close())
	})
}

func TestFileBearerToken(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")

	tests := []struct {
		name      string
		contents  *string
		wantToken string
		wantErr   string
	}{
		{
			name:      "token is presented as a bearer credential",
			contents:  ptr("header.payload.signature"),
			wantToken: "Bearer header.payload.signature",
		},
		{
			// kubelet writes projected tokens with a trailing newline.
			name:      "surrounding whitespace is trimmed",
			contents:  ptr("  header.payload.signature\n"),
			wantToken: "Bearer header.payload.signature",
		},
		{
			name:     "an empty file is rejected",
			contents: ptr("   \n"),
			wantErr:  "is empty",
		},
		{
			name:    "a missing file is rejected",
			wantErr: "read substrate token file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.contents == nil {
				require.NoError(t, os.RemoveAll(tokenFile))
			} else {
				require.NoError(t, os.WriteFile(tokenFile, []byte(*tt.contents), 0o600))
			}

			md, err := fileBearerToken(tokenFile).GetRequestMetadata(t.Context())
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantToken, md["authorization"])
		})
	}

	// A rotated token must be picked up without redialing, so the file is read
	// per call rather than cached at dial time.
	t.Run("a rewritten token file is re-read per call", func(t *testing.T) {
		creds := fileBearerToken(tokenFile)
		require.NoError(t, os.WriteFile(tokenFile, []byte("first"), 0o600))
		md, err := creds.GetRequestMetadata(t.Context())
		require.NoError(t, err)
		assert.Equal(t, "Bearer first", md["authorization"])

		require.NoError(t, os.WriteFile(tokenFile, []byte("second"), 0o600))
		md, err = creds.GetRequestMetadata(t.Context())
		require.NoError(t, err)
		assert.Equal(t, "Bearer second", md["authorization"])
	})

	// An insecure:// address must stay dialable, so the credential does not
	// demand transport security of its own.
	t.Run("transport security is not required", func(t *testing.T) {
		assert.False(t, fileBearerToken(tokenFile).RequireTransportSecurity())
	})
}

func ptr(s string) *string { return &s }
