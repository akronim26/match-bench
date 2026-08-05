// Package errors implements errors behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package errors

import "errors"

var (
	ErrDuplicateSubmission = errors.New("duplicate submission")
	ErrInvalidArtifact     = errors.New("invalid artifact")
	ErrStoreUploadFailed   = errors.New("failed to upload artifact to store")
	ErrStoreDatabaseFailed = errors.New("failed to save metadata to database")
	ErrSubmissionNotFound  = errors.New("submission not found")
	ErrInternal            = errors.New("internal server error")
	ErrValidation          = errors.New("validation failed")

	ErrCorruptArchive  = errors.New("file is not a valid ZIP archive")
	ErrNotZip          = ErrCorruptArchive
	ErrCorruptZip      = errors.New("corrupt zip archive")
	ErrTooLarge        = errors.New("file exceeds 100MB limit")
	ErrConfigMissing   = errors.New("benchmark.yaml not found at zip root")
	ErrNoBenchmarkYAML = ErrConfigMissing
	ErrNoSrcDir        = errors.New("src/ directory not found in zip")

	ErrDuplicateRootEntry    = errors.New("duplicate root entry in zip archive")
	ErrMultipleBenchmarkYAML = errors.New("multiple benchmark.yaml files at zip root")
	ErrInvalidProtocol       = errors.New("invalid protocol: must be FIX, REST, or WS")
	ErrInvalidLanguage       = errors.New("invalid language: must be cpp, rust, or go")
	ErrMissingBuildTarget    = errors.New("build.target is required in benchmark.yaml")
	ErrInvalidBuildTarget    = errors.New("build.target contains invalid characters (allowed: A-Za-z0-9 _ . -, max 64)")
	ErrInvalidPortRange      = errors.New("port out of allowed range (1024–65535)")
	ErrPortProtocolMismatch  = errors.New("port does not match the platform-mandated port for the declared protocol (FIX=9898, REST/WS=8080)")
	ErrRootConfigTooLarge    = errors.New("root config/build file exceeds 1 MiB after decompression")
	ErrMissingCMakeLists     = errors.New("cpp project must include CMakeLists.txt at zip root")
	ErrMissingCargoToml      = errors.New("rust project must include Cargo.toml at zip root")
	ErrMissingGoMod          = errors.New("go project must include go.mod at zip root")
	ErrInvalidBuildType      = errors.New("invalid build.type for the selected language")
	ErrMissingCMakeTarget    = errors.New("CMakeLists.txt has no add_executable target")
	ErrMissingCargoBin       = errors.New("Cargo.toml has no [[bin]] with the requested name")

	ErrActiveRunGroupExists = errors.New("active run-group already exists for this submission")
	ErrRunNotFound          = errors.New("run not found")
	ErrRunGroupNotFound     = errors.New("run-group not found")
	ErrScenarioNotFound     = errors.New("scenario not found")
	ErrSubmissionNotReady   = errors.New("submission is not in 'ready' status")
)
