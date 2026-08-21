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
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/klog/v2"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/sandbox-manager/infra/substrate"
	"github.com/openkruise/agents/pkg/servers/e2b/models"
	"github.com/openkruise/agents/pkg/servers/web"
)

// substrateScheme lets the buildTemplate handlers create and read ActorTemplates.
var substrateScheme = func() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(atev1alpha1.AddToScheme(scheme))
	return scheme
}()

// defaultContainerName names the generated workload container when the caller
// does not override it. E2B has no container-name concept, so a build carries
// one unnamed image and the name only has to be a stable, valid DNS label.
const defaultContainerName = "main"

// registerSubstrateRoutes registers the E2B template build endpoints. They are
// only useful on the substrate backend; on other backends the handlers reject
// with 501, so registration is unconditional to keep the route table stable.
func (sc *Controller) registerSubstrateRoutes() {
	RegisterE2BRoute(sc.mux, http.MethodPost, "/v3/templates", sc.RequestTemplateBuild, sc.CheckApiKey)
	RegisterE2BRoute(sc.mux, http.MethodPost, "/v2/templates", sc.RequestTemplateBuild, sc.CheckApiKey)
	RegisterE2BRoute(sc.mux, http.MethodPost, "/templates", sc.RequestTemplateBuild, sc.CheckApiKey)
	RegisterE2BRoute(sc.mux, http.MethodPost, "/v2/templates/{templateID}/builds/{buildID}", sc.StartTemplateBuild, sc.CheckApiKey)
	RegisterE2BRoute(sc.mux, http.MethodGet, "/templates/{templateID}/builds/{buildID}/status", sc.GetTemplateBuildStatus, sc.CheckApiKey)
}

// buildResponse builds a 202 response carrying the template and build IDs.
func buildResponse(templateID, buildID string) web.ApiResponse[models.TemplateRequestResponse] {
	return web.ApiResponse[models.TemplateRequestResponse]{
		Code: http.StatusAccepted,
		Body: models.TemplateRequestResponse{
			TemplateID: templateID,
			BuildID:    buildID,
			Aliases:    []string{templateID},
		},
	}
}

// RequestTemplateBuild handles POST /v3/templates, /v2/templates and /templates.
// It reserves the template identity and a build slot; the ActorTemplate itself
// is created when the build is started. templateID is the caller-chosen name so
// later sandbox creates can reference it, and buildID correlates the start and
// status calls.
func (sc *Controller) RequestTemplateBuild(r *http.Request) (web.ApiResponse[models.TemplateRequestResponse], *web.ApiError) {
	if apiErr := sc.requireSubstrate(); apiErr != nil {
		return web.ApiResponse[models.TemplateRequestResponse]{}, apiErr
	}

	var req models.TemplateBuildRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return web.ApiResponse[models.TemplateRequestResponse]{}, badRequest("decode template build request: %v", err)
	}
	templateID := req.TemplateName()
	if templateID == "" {
		return web.ApiResponse[models.TemplateRequestResponse]{}, badRequest("template name is required")
	}
	if errs := validateName(templateID); errs != "" {
		return web.ApiResponse[models.TemplateRequestResponse]{}, badRequest("invalid template name %q: %s", templateID, errs)
	}

	buildID := "b" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	return buildResponse(templateID, buildID), nil
}

// StartTemplateBuild handles POST /v2/templates/{templateID}/builds/{buildID}.
// It materializes the ActorTemplate from the build definition.
func (sc *Controller) StartTemplateBuild(r *http.Request) (web.ApiResponse[struct{}], *web.ApiError) {
	if apiErr := sc.requireSubstrate(); apiErr != nil {
		return web.ApiResponse[struct{}]{}, apiErr
	}
	user := GetUserFromContext(r.Context())
	if user == nil {
		return web.ApiResponse[struct{}]{}, &web.ApiError{Code: http.StatusUnauthorized, Message: "user not found"}
	}
	templateID := r.PathValue("templateID")
	buildID := r.PathValue("buildID")
	namespace := sc.getNamespaceOfUser(user)

	var req models.TemplateBuildStart
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return web.ApiResponse[struct{}]{}, badRequest("decode build start request: %v", err)
	}
	if err := req.ParseExtensions(r.Header); err != nil {
		return web.ApiResponse[struct{}]{}, badRequest("%v", err)
	}

	tmpl, apiErr := sc.buildActorTemplate(namespace, templateID, buildID, req)
	if apiErr != nil {
		return web.ApiResponse[struct{}]{}, apiErr
	}

	log := klog.FromContext(r.Context())
	if err := sc.substrateClient.Create(r.Context(), tmpl); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// A retried start for the same build is idempotent.
			return web.ApiResponse[struct{}]{Code: http.StatusAccepted}, nil
		}
		log.Error(err, "failed to create actor template", "template", templateID, "build", buildID)
		return web.ApiResponse[struct{}]{}, &web.ApiError{
			Code:    http.StatusInternalServerError,
			Message: fmt.Sprintf("create actor template: %v", err),
		}
	}
	log.Info("actor template build started", "template", templateID, "build", buildID, "name", tmpl.Name)
	return web.ApiResponse[struct{}]{Code: http.StatusAccepted}, nil
}

// GetTemplateBuildStatus handles GET /templates/{templateID}/builds/{buildID}/status.
func (sc *Controller) GetTemplateBuildStatus(r *http.Request) (web.ApiResponse[models.TemplateBuildInfo], *web.ApiError) {
	if apiErr := sc.requireSubstrate(); apiErr != nil {
		return web.ApiResponse[models.TemplateBuildInfo]{}, apiErr
	}
	user := GetUserFromContext(r.Context())
	if user == nil {
		return web.ApiResponse[models.TemplateBuildInfo]{}, &web.ApiError{Code: http.StatusUnauthorized, Message: "user not found"}
	}
	templateID := r.PathValue("templateID")
	buildID := r.PathValue("buildID")
	namespace := sc.getNamespaceOfUser(user)

	resolver := substrate.NewTemplateResolver(sc.substrateClient)
	tmpl, err := resolver.GetBuild(r.Context(), namespace, templateID, buildID)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return web.ApiResponse[models.TemplateBuildInfo]{}, &web.ApiError{
				Code:    http.StatusNotFound,
				Message: fmt.Sprintf("build %s of template %s not found", buildID, templateID),
			}
		}
		return web.ApiResponse[models.TemplateBuildInfo]{}, &web.ApiError{
			Code:    http.StatusInternalServerError,
			Message: fmt.Sprintf("get build status: %v", err),
		}
	}

	return web.ApiResponse[models.TemplateBuildInfo]{
		Code: http.StatusOK,
		Body: models.TemplateBuildInfo{
			TemplateID: templateID,
			BuildID:    buildID,
			Status:     string(substrate.BuildStatusOf(tmpl)),
			Logs:       buildStatusLogs(tmpl),
		},
	}, nil
}

// buildActorTemplate maps an E2B build definition onto an ActorTemplate. It
// rejects build features substrate cannot honor rather than dropping them: a
// silently ignored build step produces an image that does not match what the
// caller asked for.
func (sc *Controller) buildActorTemplate(
	namespace, templateID, buildID string,
	req models.TemplateBuildStart,
) (*atev1alpha1.ActorTemplate, *web.ApiError) {
	if len(req.Steps) > 0 {
		return nil, badRequest("the substrate backend does not run build steps; " +
			"provide a prebuilt image via fromImage instead")
	}
	if req.FromTemplate != "" {
		return nil, badRequest("fromTemplate is not supported by the substrate backend; use fromImage")
	}
	if req.FromImage == "" {
		return nil, badRequest("fromImage is required")
	}
	// Substrate pins every image by digest because a tag change would silently
	// invalidate snapshots.
	if !strings.Contains(req.FromImage, "@") {
		return nil, badRequest("fromImage %q must be pinned by digest (contain '@sha256:...')", req.FromImage)
	}
	if sc.substrate.PauseImage == "" {
		return nil, &web.ApiError{
			Code:    http.StatusInternalServerError,
			Message: "substrate pause image is not configured",
		}
	}

	container := atev1alpha1.Container{Name: defaultContainerName, Image: req.FromImage}
	if req.Extensions.ContainerName != "" {
		container.Name = req.Extensions.ContainerName
	}
	if req.StartCmd != "" {
		container.Command = []string{"/bin/sh", "-c", req.StartCmd}
	}
	readyz, apiErr := parseReadyCmd(req.ReadyCmd)
	if apiErr != nil {
		return nil, apiErr
	}
	container.Readyz = readyz

	snapshotsLocation := sc.snapshotsLocation(namespace)
	if req.Extensions.SnapshotsLocation != "" {
		snapshotsLocation = req.Extensions.SnapshotsLocation
	}

	tmpl := &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      templateID + "-" + buildID,
			Namespace: namespace,
			Labels: map[string]string{
				agentsv1alpha1.LabelE2BTemplateID: templateID,
				agentsv1alpha1.LabelE2BBuildID:    buildID,
			},
		},
		Spec: atev1alpha1.ActorTemplateSpec{
			PauseImage:      sc.substrate.PauseImage,
			Containers:      []atev1alpha1.Container{container},
			SandboxClass:    atev1alpha1.SandboxClass(sc.substrate.SandboxClass),
			SnapshotsConfig: atev1alpha1.SnapshotsConfig{Location: snapshotsLocation},
		},
	}
	// A nil selector leaves every worker pool eligible, so only set it when the
	// caller asked to narrow the set.
	if len(req.Extensions.WorkerSelector) > 0 {
		tmpl.Spec.WorkerSelector = &metav1.LabelSelector{
			MatchLabels: req.Extensions.WorkerSelector,
		}
	}
	return tmpl, nil
}

// snapshotsLocation joins the configured base with the team namespace so each
// team's snapshots stay separated.
func (sc *Controller) snapshotsLocation(namespace string) string {
	base := strings.TrimSuffix(sc.substrate.SnapshotsLocationBase, "/")
	return fmt.Sprintf("%s/%s/", base, namespace)
}

// parseReadyCmd maps the E2B readyCmd onto a substrate HTTP readiness probe.
// Only HTTP-shaped ready checks map cleanly; anything else is rejected rather
// than guessed, because a wrong readiness probe silently defeats readiness.
func parseReadyCmd(readyCmd string) (*atev1alpha1.ContainerReadyz, *web.ApiError) {
	readyCmd = strings.TrimSpace(readyCmd)
	if readyCmd == "" {
		return nil, nil
	}
	path, port, ok := parseHTTPReadyCmd(readyCmd)
	if !ok {
		return nil, badRequest("the substrate backend supports only HTTP readiness checks; "+
			"use wait_for_url or wait_for_port, got %q", readyCmd)
	}
	return &atev1alpha1.ContainerReadyz{
		HTTPGet: &atev1alpha1.HTTPGetAction{Path: path, Port: port},
	}, nil
}

// requireSubstrate rejects a request when the substrate backend is disabled.
func (sc *Controller) requireSubstrate() *web.ApiError {
	if !sc.substrate.Enabled() || sc.substrateClient == nil {
		return &web.ApiError{
			Code:    http.StatusNotImplemented,
			Message: "template build is only available on the substrate backend",
		}
	}
	return nil
}

// buildStatusLogs surfaces the ActorTemplate readiness condition message as a
// single build log line so a polling client sees why a build is not ready.
func buildStatusLogs(tmpl *atev1alpha1.ActorTemplate) []string {
	for _, cond := range tmpl.Status.Conditions {
		if cond.Message != "" {
			return []string{cond.Message}
		}
	}
	return nil
}

func badRequest(format string, a ...any) *web.ApiError {
	return &web.ApiError{Code: http.StatusBadRequest, Message: fmt.Sprintf(format, a...)}
}

// validateName checks that a template name is a DNS-1123 label, since it is
// used as the ActorTemplate name prefix. It returns a joined error string, or
// empty when valid.
func validateName(name string) string {
	if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
		return strings.Join(errs, ", ")
	}
	return ""
}

// parseHTTPReadyCmd extracts an HTTP path and port from the ready-check forms
// the E2B SDK emits. It recognizes a bare URL, a curl command carrying a URL,
// and a wait_for_port style port check. It reports ok=false for anything it
// cannot map to an HTTP probe.
func parseHTTPReadyCmd(readyCmd string) (path string, port int32, ok bool) {
	if u := extractURL(readyCmd); u != "" {
		parsed, err := url.Parse(u)
		if err != nil {
			return "", 0, false
		}
		p := parsed.Port()
		if p == "" {
			return "", 0, false
		}
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return "", 0, false
		}
		reqPath := parsed.Path
		if reqPath == "" {
			reqPath = "/readyz"
		}
		return reqPath, int32(n), true
	}

	// wait_for_port(N) checks only a listening port; substrate probes HTTP, so
	// default the path to /readyz.
	if n, found := extractPort(readyCmd); found {
		return "/readyz", n, true
	}
	return "", 0, false
}

// extractURL returns the first http(s) URL token in the command, or empty.
func extractURL(cmd string) string {
	for _, field := range strings.Fields(cmd) {
		field = strings.Trim(field, "'\"")
		if strings.HasPrefix(field, "http://") || strings.HasPrefix(field, "https://") {
			return field
		}
	}
	return ""
}

// extractPort returns a port from a "port:N" or bare "N" ready command.
func extractPort(cmd string) (int32, bool) {
	cmd = strings.TrimSpace(cmd)
	cmd = strings.TrimPrefix(cmd, "port:")
	for _, field := range strings.Fields(cmd) {
		if n, err := strconv.Atoi(field); err == nil && n >= 1 && n <= 65535 {
			return int32(n), true
		}
	}
	return 0, false
}
