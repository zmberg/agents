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

package models

import "time"

// Template represents an E2B template
type Template struct {
	TemplateID    string     `json:"templateID"`
	Public        bool       `json:"public"`
	Aliases       []string   `json:"aliases"`
	Names         []string   `json:"names"`
	CreatedAt     time.Time  `json:"createdAt"`
	UpdatedAt     time.Time  `json:"updatedAt"`
	LastSpawnedAt *time.Time `json:"lastSpawnedAt"`
	SpawnCount    int64      `json:"spawnCount"`
	Builds        []Build    `json:"builds"`
}

// TemplateInfo represents simplified template information for list response
type TemplateInfo struct {
	TemplateID    string     `json:"templateID"`
	BuildID       string     `json:"buildID"`
	CPUCount      int        `json:"cpuCount"`
	MemoryMB      int        `json:"memoryMB"`
	DiskSizeMB    int        `json:"diskSizeMB"`
	Public        bool       `json:"public"`
	Aliases       []string   `json:"aliases"`
	Names         []string   `json:"names"`
	CreatedAt     time.Time  `json:"createdAt"`
	UpdatedAt     time.Time  `json:"updatedAt"`
	CreatedBy     *TeamUser  `json:"createdBy"`
	LastSpawnedAt *time.Time `json:"lastSpawnedAt"`
	SpawnCount    int64      `json:"spawnCount"`
	BuildCount    int        `json:"buildCount"`
	EnvdVersion   string     `json:"envdVersion"`
	BuildStatus   string     `json:"buildStatus"`
}

// Build represents a build of a template
type Build struct {
	BuildID     string    `json:"buildID"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	CPUCount    int       `json:"cpuCount"`
	MemoryMB    int       `json:"memoryMB"`
	FinishedAt  time.Time `json:"finishedAt"`
	DiskSizeMB  int       `json:"diskSizeMB"`
	EnvdVersion string    `json:"envdVersion"`
}

// TemplateBuildRequest is the body of POST /v3/templates (and the deprecated
// alias/teamID form). It reserves a template identity and a build slot; the
// build content is submitted separately to the start endpoint.
type TemplateBuildRequest struct {
	// Name is the template name. It may carry a "name:tag" suffix.
	Name string `json:"name"`
	// Alias is the deprecated predecessor of Name; accepted for older SDKs.
	Alias    string `json:"alias"`
	CPUCount int    `json:"cpuCount"`
	MemoryMB int    `json:"memoryMB"`
}

// TemplateName returns the effective template name, preferring Name and falling
// back to the deprecated Alias. Any tag suffix on Name is stripped.
func (r TemplateBuildRequest) TemplateName() string {
	name := r.Name
	if name == "" {
		name = r.Alias
	}
	if idx := indexByte(name, ':'); idx >= 0 {
		name = name[:idx]
	}
	return name
}

// TemplateRequestResponse is the 202 body returned when a build is requested.
type TemplateRequestResponse struct {
	TemplateID string   `json:"templateID"`
	BuildID    string   `json:"buildID"`
	Aliases    []string `json:"aliases,omitempty"`
}

// TemplateStep is one step in an E2B build (RUN, COPY, ...). The substrate
// backend does not run a build pipeline, so only base-image forms are honored
// and any steps are rejected.
type TemplateStep struct {
	Type      string   `json:"type"`
	Args      []string `json:"args"`
	FilesHash string   `json:"filesHash"`
	Force     bool     `json:"force"`
}

// TemplateBuildStart is the body of POST /v2/templates/{id}/builds/{buildID}.
// It carries the actual build definition.
type TemplateBuildStart struct {
	FromImage    string         `json:"fromImage"`
	FromTemplate string         `json:"fromTemplate"`
	Force        bool           `json:"force"`
	Steps        []TemplateStep `json:"steps"`
	StartCmd     string         `json:"startCmd"`
	ReadyCmd     string         `json:"readyCmd"`
}

// TemplateBuildInfo is the body of GET /templates/{id}/builds/{buildID}/status.
type TemplateBuildInfo struct {
	TemplateID string   `json:"templateID"`
	BuildID    string   `json:"buildID"`
	Status     string   `json:"status"`
	Logs       []string `json:"logs"`
}

// indexByte returns the index of the first occurrence of c in s, or -1.
func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}
