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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	managererrors "github.com/openkruise/agents/pkg/sandbox-manager/errors"
)

// unsupported returns the domain error the API layer maps to a clear response
// for a capability the substrate backend does not implement.
func unsupported(capability string) error {
	return managererrors.NewError(managererrors.ErrorNotAllowed,
		"%s are not supported by the substrate backend", capability)
}

// errNoControlClient reports that the backend was built without a control-plane
// connection, which is a configuration error rather than a runtime failure.
func errNoControlClient() error {
	return managererrors.NewError(managererrors.ErrorInternal,
		"substrate control client is not configured")
}

// isNotFound reports whether a control-plane error means the actor is gone. A
// gone actor is success for delete and a definitive miss for get.
func isNotFound(err error) bool {
	return status.Code(err) == codes.NotFound
}

// isAlreadyHibernated reports whether a pause or suspend failed only because the
// actor already reached that state. FailedPrecondition is substrate's signal
// that the requested transition is a no-op from the current state.
func isAlreadyHibernated(err error) bool {
	return status.Code(err) == codes.FailedPrecondition
}
