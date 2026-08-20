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

package e2b

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/servers/e2b/models"
)

const pinnedImage = "registry.example.com/counter@sha256:" +
	"d930424a2618e0277fc9dd79a7b37ed8fa3f85a839057f72426dc4c1031a458f"

func substrateController() *Controller {
	return &Controller{
		substrate: &SubstrateConfig{
			Address:               "insecure://substrate:50051",
			PauseImage:            "registry.k8s.io/pause@sha256:abc",
			SnapshotsLocationBase: "s3://snapshots",
			SandboxClass:          "gvisor",
		},
	}
}

func TestTemplateBuildRequestName(t *testing.T) {
	tests := []struct {
		name string
		req  models.TemplateBuildRequest
		want string
	}{
		{name: "name preferred", req: models.TemplateBuildRequest{Name: "counter", Alias: "old"}, want: "counter"},
		{name: "falls back to alias", req: models.TemplateBuildRequest{Alias: "counter"}, want: "counter"},
		{name: "tag suffix stripped", req: models.TemplateBuildRequest{Name: "counter:v1"}, want: "counter"},
		{name: "empty", req: models.TemplateBuildRequest{}, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.req.TemplateName())
		})
	}
}

func TestBuildActorTemplate(t *testing.T) {
	sc := substrateController()

	t.Run("maps a pinned fromImage onto the actor template", func(t *testing.T) {
		tmpl, apiErr := sc.buildActorTemplate("team-a", "counter", "b1", models.TemplateBuildStart{
			FromImage: pinnedImage,
			StartCmd:  "/ko-app/counter",
		})
		require.Nil(t, apiErr)
		assert.Equal(t, "counter-b1", tmpl.Name)
		assert.Equal(t, "team-a", tmpl.Namespace)
		assert.Equal(t, "counter", tmpl.Labels[agentsv1alpha1.LabelE2BTemplateID])
		assert.Equal(t, "b1", tmpl.Labels[agentsv1alpha1.LabelE2BBuildID])
		require.Len(t, tmpl.Spec.Containers, 1)
		assert.Equal(t, pinnedImage, tmpl.Spec.Containers[0].Image)
		assert.Equal(t, []string{"/bin/sh", "-c", "/ko-app/counter"}, tmpl.Spec.Containers[0].Command)
		assert.Equal(t, "registry.k8s.io/pause@sha256:abc", tmpl.Spec.PauseImage)
		// Snapshots are separated per team namespace.
		assert.Equal(t, "s3://snapshots/team-a/", tmpl.Spec.SnapshotsConfig.Location)
	})

	// Substrate does not run a build pipeline, so honoring steps would produce
	// an image that does not match the request. Reject instead.
	t.Run("rejects build steps", func(t *testing.T) {
		_, apiErr := sc.buildActorTemplate("team-a", "counter", "b1", models.TemplateBuildStart{
			FromImage: pinnedImage,
			Steps:     []models.TemplateStep{{Type: "RUN", Args: []string{"apt install"}}},
		})
		require.NotNil(t, apiErr)
		assert.Contains(t, apiErr.Message, "does not run build steps")
	})

	t.Run("rejects fromTemplate", func(t *testing.T) {
		_, apiErr := sc.buildActorTemplate("team-a", "counter", "b1", models.TemplateBuildStart{
			FromTemplate: "base",
		})
		require.NotNil(t, apiErr)
		assert.Contains(t, apiErr.Message, "fromTemplate is not supported")
	})

	t.Run("rejects missing fromImage", func(t *testing.T) {
		_, apiErr := sc.buildActorTemplate("team-a", "counter", "b1", models.TemplateBuildStart{})
		require.NotNil(t, apiErr)
		assert.Contains(t, apiErr.Message, "fromImage is required")
	})

	// Substrate pins every image by digest because a tag change silently
	// invalidates snapshots.
	t.Run("rejects an unpinned image", func(t *testing.T) {
		_, apiErr := sc.buildActorTemplate("team-a", "counter", "b1", models.TemplateBuildStart{
			FromImage: "registry.example.com/counter:latest",
		})
		require.NotNil(t, apiErr)
		assert.Contains(t, apiErr.Message, "pinned by digest")
	})

	t.Run("fails when the pause image is not configured", func(t *testing.T) {
		bare := &Controller{substrate: &SubstrateConfig{Address: "insecure://s", SnapshotsLocationBase: "s3://x"}}
		_, apiErr := bare.buildActorTemplate("team-a", "counter", "b1", models.TemplateBuildStart{
			FromImage: pinnedImage,
		})
		require.NotNil(t, apiErr)
		assert.Contains(t, apiErr.Message, "pause image is not configured")
	})

	t.Run("maps an HTTP readyCmd onto a readiness probe", func(t *testing.T) {
		tmpl, apiErr := sc.buildActorTemplate("team-a", "counter", "b1", models.TemplateBuildStart{
			FromImage: pinnedImage,
			ReadyCmd:  "curl http://localhost:8080/healthz",
		})
		require.Nil(t, apiErr)
		require.NotNil(t, tmpl.Spec.Containers[0].Readyz)
		assert.Equal(t, "/healthz", tmpl.Spec.Containers[0].Readyz.HTTPGet.Path)
		assert.Equal(t, int32(8080), tmpl.Spec.Containers[0].Readyz.HTTPGet.Port)
	})
}

func TestParseReadyCmd(t *testing.T) {
	tests := []struct {
		name     string
		readyCmd string
		wantNil  bool
		wantErr  bool
		wantPath string
		wantPort int32
	}{
		{name: "empty yields no probe", readyCmd: "", wantNil: true},
		{
			name:     "curl URL",
			readyCmd: "curl http://localhost:3000/health",
			wantPath: "/health", wantPort: 3000,
		},
		{
			name:     "bare URL without path defaults to /readyz",
			readyCmd: "http://localhost:8000",
			wantPath: "/readyz", wantPort: 8000,
		},
		{
			// wait_for_port only knows the port; substrate probes HTTP, so the
			// path defaults.
			name:     "port-only check",
			readyCmd: "port:8080",
			wantPath: "/readyz", wantPort: 8080,
		},
		{
			// wait_for_process / wait_for_file cannot map to an HTTP probe.
			name:     "process check rejected",
			readyCmd: "pgrep nginx",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readyz, apiErr := parseReadyCmd(tt.readyCmd)
			if tt.wantErr {
				require.NotNil(t, apiErr)
				return
			}
			require.Nil(t, apiErr)
			if tt.wantNil {
				assert.Nil(t, readyz)
				return
			}
			require.NotNil(t, readyz)
			assert.Equal(t, tt.wantPath, readyz.HTTPGet.Path)
			assert.Equal(t, tt.wantPort, readyz.HTTPGet.Port)
		})
	}
}

func TestSubstrateConfigEnabled(t *testing.T) {
	assert.False(t, (*SubstrateConfig)(nil).Enabled())
	assert.False(t, (&SubstrateConfig{}).Enabled())
	assert.True(t, (&SubstrateConfig{Address: "insecure://s"}).Enabled())
}

func TestSnapshotsLocation(t *testing.T) {
	sc := &Controller{substrate: &SubstrateConfig{SnapshotsLocationBase: "s3://bucket/snaps/"}}
	// A trailing slash on the base must not double up.
	assert.Equal(t, "s3://bucket/snaps/team-a/", sc.snapshotsLocation("team-a"))
}
