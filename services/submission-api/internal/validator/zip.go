// Package validator implements zip behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package validator

import (
	"archive/zip"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/iicpc/schemas/topics"
	cerrs "github.com/iicpc/submission-api/internal/errors"
	"gopkg.in/yaml.v3"
)

const MaxZipBytes = 100 << 20      // 100 MB
const maxRootConfigBytes = 1 << 20 // 1 MB per root config/build file after decompression

// ProtocolAll is the sentinel a submission declares in benchmark.yaml to
// offer all three transports (FIX + REST + WS) to the same contestant
// simultaneously. See docs/tps-improvement-plan.md §7.3.
const ProtocolAll = "ALL"

// Protocol declarations are validated by topics.ParseProtocols: one protocol,
// "ALL", or an ordered comma combo ("REST,WS"). Order is meaningful — the
// first element is the primary protocol pass-1 correctness runs on.
var validLanguages = map[string]struct{}{"cpp": {}, "rust": {}, "go": {}}

// BuildSection groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type BuildSection struct {
	Type   string `yaml:"type"`   // cmake | cargo | go
	Target string `yaml:"target"` // binary name to produce
}

// BenchmarkConfig groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type BenchmarkConfig struct {
	Protocol string       `yaml:"protocol"`
	Language string       `yaml:"language"`
	Build    BuildSection `yaml:"build"`
	Port     int          `yaml:"port"`
	TeamName string       `yaml:"team_name"` // optional until auth is added
}

// ValidateSubmissionZip performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func ValidateSubmissionZip(r io.ReaderAt, size int64) (*BenchmarkConfig, error) {
	if size > MaxZipBytes {
		return nil, cerrs.ErrTooLarge
	}

	var header [4]byte
	if _, err := r.ReadAt(header[:], 0); err != nil {
		return nil, cerrs.ErrNotZip
	}
	if header[0] != 0x50 || header[1] != 0x4B || header[2] != 0x03 || header[3] != 0x04 {
		return nil, cerrs.ErrNotZip
	}

	zr, err := zip.NewReader(r, size)
	if err != nil {
		return nil, cerrs.ErrCorruptZip
	}

	var cfg BenchmarkConfig
	var foundBenchmark, foundSrc bool
	var buildFileContent []byte
	var buildFileName string
	rootEntries := make(map[string]struct{})

	for _, f := range zr.File {
		if strings.HasPrefix(f.Name, "src/") {
			foundSrc = true
		}

		if strings.ContainsRune(f.Name, '/') {
			continue
		}
		if _, seen := rootEntries[f.Name]; seen {
			return nil, cerrs.ErrDuplicateRootEntry
		}
		rootEntries[f.Name] = struct{}{}

		switch f.Name {
		case "benchmark.yaml", "benchmark.yml":
			if foundBenchmark {
				return nil, cerrs.ErrMultipleBenchmarkYAML
			}
			rc, err := f.Open()
			if err != nil {
				return nil, fmt.Errorf("open benchmark.yaml: %w", err)
			}
			benchmarkData, readErr := readLimitedRootFile(rc)
			rc.Close()
			if readErr != nil {
				return nil, fmt.Errorf("read benchmark.yaml: %w", readErr)
			}
			if decodeErr := yaml.Unmarshal(benchmarkData, &cfg); decodeErr != nil {
				return nil, fmt.Errorf("invalid benchmark.yaml: %w", decodeErr)
			}
			foundBenchmark = true

		case "CMakeLists.txt", "Cargo.toml", "go.mod":
			if buildFileName != "" {
				return nil, fmt.Errorf("multiple build files at zip root: %s and %s", buildFileName, f.Name)
			}
			rc, err := f.Open()
			if err != nil {
				return nil, fmt.Errorf("open %s: %w", f.Name, err)
			}
			buildFileContent, err = readLimitedRootFile(rc)
			rc.Close()
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", f.Name, err)
			}
			buildFileName = f.Name
		}
	}

	if !foundBenchmark {
		return nil, cerrs.ErrNoBenchmarkYAML
	}
	if !foundSrc {
		return nil, cerrs.ErrNoSrcDir
	}

	if _, err := topics.ParseProtocols(cfg.Protocol); err != nil {
		return nil, cerrs.ErrInvalidProtocol
	}
	if _, ok := validLanguages[cfg.Language]; !ok {
		return nil, cerrs.ErrInvalidLanguage
	}
	if cfg.Build.Target == "" {
		return nil, cerrs.ErrMissingBuildTarget
	}
	port, err := normalizePort(cfg)
	if err != nil {
		return nil, err
	}
	cfg.Port = port

	if err := validateBuildTarget(&cfg, buildFileName, buildFileContent); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// readLimitedRootFile performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func readLimitedRootFile(r io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxRootConfigBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxRootConfigBytes {
		return nil, cerrs.ErrRootConfigTooLarge
	}
	return data, nil
}

var validBuildTargetName = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

// validatePortPolicy enforces the platform-mandated port table: FIX must
// declare 9898, REST/WS must declare 8080. Ports are platform constants, not
// contestant-chosen (docs/tps-improvement-plan.md §7.3). A submission
// declaring ProtocolAll offers all protocols on their respective mandated
// ports, so its declared port is not checked against a single value.
// normalizePort resolves benchmark.yaml's OPTIONAL `port:` (decision
// 2026-08-02): ports are platform-mandated per protocol (FIX=9898,
// REST/WS=8080 — the eBPF capture filter hardcodes them, so they were never
// contestant-choosable). Absent → derived from the PRIMARY (first-declared)
// protocol. Present → still validated: a wrong value on a single-protocol
// declaration means a confused contestant, better told loudly at upload than
// debugged at run time.
func normalizePort(cfg BenchmarkConfig) (int, error) {
	if cfg.Port == 0 {
		parts, err := topics.ParseProtocols(cfg.Protocol)
		if err != nil {
			return 0, cerrs.ErrInvalidProtocol
		}
		return int(topics.PortForProtocol(parts[0])), nil
	}
	if cfg.Port < 1024 || cfg.Port > 65535 {
		return 0, cerrs.ErrInvalidPortRange
	}
	if err := validatePortPolicy(cfg.Protocol, cfg.Port); err != nil {
		return 0, err
	}
	return cfg.Port, nil
}

func validatePortPolicy(protocol string, port int) error {
	parts, err := topics.ParseProtocols(protocol)
	if err != nil {
		return cerrs.ErrInvalidProtocol
	}
	// Multi-protocol (ALL or any combo) serves each protocol on its own
	// mandated port, so the single declared port is not checked.
	if len(parts) > 1 {
		return nil
	}
	if uint16(port) != topics.PortForProtocol(parts[0]) {
		return cerrs.ErrPortProtocolMismatch
	}
	return nil
}

// validateBuildTarget performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func validateBuildTarget(cfg *BenchmarkConfig, buildFileName string, buildFileContent []byte) error {
	if !validBuildTargetName.MatchString(cfg.Build.Target) {
		return cerrs.ErrInvalidBuildTarget
	}
	switch cfg.Language {
	case "cpp":
		if cfg.Build.Type != "cmake" {
			return cerrs.ErrInvalidBuildType
		}
		if buildFileName != "CMakeLists.txt" {
			return cerrs.ErrMissingCMakeLists
		}
		hasTarget, err := cmakeHasTarget(buildFileContent, cfg.Build.Target)
		if err != nil {
			return err
		}
		if !hasTarget {
			return cerrs.ErrMissingCMakeTarget
		}

	case "rust":
		if cfg.Build.Type != "cargo" {
			return cerrs.ErrInvalidBuildType
		}
		if buildFileName != "Cargo.toml" {
			return cerrs.ErrMissingCargoToml
		}
		if !cargoHasBin(buildFileContent, cfg.Build.Target) {
			return cerrs.ErrMissingCargoBin
		}

	case "go":
		if cfg.Build.Type != "go" {
			return cerrs.ErrInvalidBuildType
		}
		if buildFileName != "go.mod" {
			return cerrs.ErrMissingGoMod
		}
	}

	return nil
}

// cmakeHasTarget performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func cmakeHasTarget(data []byte, target string) (bool, error) {
	pattern := `(?im)^\s*add_executable\s*\(\s*` + regexp.QuoteMeta(target) + `[\s),]`
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false, fmt.Errorf("invalid generated CMake target regex: %w", err)
	}
	return re.Match(data), nil
}

// cargoHasBin performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func cargoHasBin(data []byte, target string) bool {
	var manifest struct {
		Bin []struct {
			Name string `toml:"name"`
		} `toml:"bin"`
	}
	if err := toml.Unmarshal(data, &manifest); err != nil {
		return false
	}
	for _, bin := range manifest.Bin {
		if bin.Name == target {
			return true
		}
	}
	return false
}
