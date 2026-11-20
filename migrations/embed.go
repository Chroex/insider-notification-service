// Package migrations embeds the versioned SQL files so tests (and any
// future in-process migrator) apply exactly the files golang-migrate runs.
package migrations

import "embed"

// FS holds every *.sql file in this directory.
//
//go:embed *.sql
var FS embed.FS
