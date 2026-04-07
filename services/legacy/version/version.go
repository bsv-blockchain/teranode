// Copyright (c) 2013-2014 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package version provides the application version for the legacy service.
// The version is sourced from the build-injected value set via gocore.SetInfo()
// during daemon startup, ensuring consistency with the main Teranode binary.
package version

import (
	"strings"

	"github.com/ordishs/gocore"
)

// String returns the application version as set by the build process.
// It retrieves the version from gocore, which is populated at startup
// via ldflags (e.g. "v1.2.3" from a git tag or "v0.0.0-20260407-abc1234").
// The leading "v" prefix is stripped if present.
func String() string {
	return strings.TrimPrefix(gocore.GetVersion(), "v")
}
