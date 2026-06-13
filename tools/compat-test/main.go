// Temporary compatibility test utility.
// Discovers all RouterOS 7.x CHR images, boots each in QEMU, runs the
// integration test suite against it, and writes a compatibility matrix.
//
// Usage: go run ./tools/compat-test/ [flags]
//
//	-cache-dir      DIR   where to keep downloaded .img files (default: /tmp/chr-compat-cache)
//	-parallel       N     concurrent VMs (default: 4)
//	-report         FILE  markdown report output (default: compat-report.md)
//	-json           FILE  JSON report output     (default: compat-report.json)
//	-versions       LIST  comma-separated versions to test instead of auto-discovery
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	downloadBase = "https://download.mikrotik.com/routeros"
	testPassword = "ChrTest1!"
	basePort     = 14480 // host port base; each slot uses basePort + slot*10
	portStep     = 10
)

// ── Version discovery ──────────────────────────────────────────────────────

func chrURL(v string) string {
	return fmt.Sprintf("%s/%s/chr-%s.img.zip", downloadBase, v, v)
}

// candidateVersions generates all plausible RouterOS 7.x version strings to probe.
func candidateVersions() []string {
	var out []string
	for minor := 0; minor <= 30; minor++ {
		base := fmt.Sprintf("7.%d", minor)
		out = append(out, base)
		for patch := 1; patch <= 15; patch++ {
			out = append(out, fmt.Sprintf("%s.%d", base, patch))
		}
		for _, suffix := range []string{
			"beta1", "beta2", "beta3", "beta4", "beta5",
			"rc1", "rc2", "rc3", "rc4", "rc5",
			"testing1", "testing2",
		} {
			out = append(out, base+suffix)
		}
	}
	return out
}

func probeExists(ctx context.Context, v string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, chrURL(v), nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func discoverVersions(ctx context.Context, workers int) []string {
	candidates := candidateVersions()
	var (
		mu    sync.Mutex
		found []string
		wg    sync.WaitGroup
		sem   = make(chan struct{}, workers)
	)
	for _, v := range candidates {
		wg.Add(1)
		go func(version string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if probeExists(ctx, version) {
				mu.Lock()
				found = append(found, version)
				mu.Unlock()
			}
		}(v)
	}
	wg.Wait()
	sort.Slice(found, func(i, j int) bool { return versionLess(found[i], found[j]) })
	return found
}

type verParts struct {
	minor, patch int
	suffix       string
}

func parseVer(v string) verParts {
	rest := strings.TrimPrefix(v, "7.")
	i := strings.IndexFunc(rest, func(r rune) bool { return r != '.' && (r < '0' || r > '9') })
	vp := verParts{}
	numPart := rest
	if i >= 0 {
		vp.suffix = rest[i:]
		numPart = rest[:i]
	}
	parts := strings.SplitN(numPart, ".", 2)
	_, _ = fmt.Sscan(parts[0], &vp.minor)
	if len(parts) == 2 {
		_, _ = fmt.Sscan(parts[1], &vp.patch)
	}
	return vp
}

// suffixRank: pre-releases sort before the corresponding stable (no suffix).
func suffixRank(s string) int {
	switch {
	case strings.HasPrefix(s, "beta"):
		return 1
	case strings.HasPrefix(s, "testing"):
		return 2
	case strings.HasPrefix(s, "rc"):
		return 3
	default:
		return 4
	}
}

func versionLess(a, b string) bool {
	pa, pb := parseVer(a), parseVer(b)
	if pa.minor != pb.minor {
		return pa.minor < pb.minor
	}
	if pa.patch != pb.patch {
		return pa.patch < pb.patch
	}
	return suffixRank(pa.suffix) < suffixRank(pb.suffix)
}

// ── Image management ───────────────────────────────────────────────────────

func ensureImage(ctx context.Context, version, cacheDir string) (string, error) {
	imgPath := filepath.Join(cacheDir, "chr-"+version+".img")
	if _, err := os.Stat(imgPath); err == nil {
		return imgPath, nil
	}

	zipPath := imgPath + ".zip"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, chrURL(version), nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("download: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download: HTTP %d", resp.StatusCode)
	}

	f, err := os.Create(zipPath)
	if err != nil {
		return "", err
	}
	_, err = io.Copy(f, resp.Body)
	f.Close()
	if err != nil {
		os.Remove(zipPath)
		return "", fmt.Errorf("write zip: %w", err)
	}

	out, err := exec.CommandContext(ctx, "unzip", "-o", "-j", zipPath, "*.img", "-d", cacheDir).CombinedOutput()
	os.Remove(zipPath)
	if err != nil {
		return "", fmt.Errorf("unzip: %s: %w", strings.TrimSpace(string(out)), err)
	}
	if _, err := os.Stat(imgPath); err != nil {
		return "", fmt.Errorf("expected %s not found after unzip", filepath.Base(imgPath))
	}
	return imgPath, nil
}

// ── VM lifecycle ───────────────────────────────────────────────────────────

func bootVM(imgPath string, port int) (pidFile string, err error) {
	pidFile = fmt.Sprintf("/tmp/chr-compat-%d.pid", port)
	cmd := exec.Command("qemu-system-x86_64",
		"-enable-kvm", "-m", "256",
		"-hda", imgPath, "-snapshot",
		"-net", "nic",
		"-net", fmt.Sprintf("user,hostfwd=tcp::%d-:80", port),
		"-display", "none",
		"-daemonize", "-pidfile", pidFile,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("qemu: %s: %w", strings.TrimSpace(string(out)), err)
	}
	return pidFile, nil
}

func killVM(pidFile string) {
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return
	}
	_ = exec.Command("kill", strings.TrimSpace(string(data))).Run()
	os.Remove(pidFile)
}

func waitReady(ctx context.Context, port int) (float64, error) {
	url := fmt.Sprintf("http://127.0.0.1:%d/rest/system/resource", port)
	start := time.Now()
	client := &http.Client{Timeout: 3 * time.Second}
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		req.SetBasicAuth("admin", "")
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return time.Since(start).Seconds(), nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("API not ready after %.0fs: %w", time.Since(start).Seconds(), ctx.Err())
		case <-time.After(5 * time.Second):
		}
	}
}

func setPassword(ctx context.Context, port int) error {
	body := `{"old-password":"","new-password":"` + testPassword + `","confirm-new-password":"` + testPassword + `"}`
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/rest/password", port),
		strings.NewReader(body))
	req.SetBasicAuth("admin", "")
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

// ── Test execution ─────────────────────────────────────────────────────────

type testEvent struct {
	Action string `json:"Action"`
	Test   string `json:"Test"`
}

func runTests(ctx context.Context, port int, projectDir string) (map[string]string, float64, error) {
	start := time.Now()
	cmd := exec.CommandContext(ctx, "go", "test", "-json", "-tags", "integration",
		"./internal/mikrotik/", "-timeout", "300s")
	cmd.Dir = projectDir
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("MIKROTIK_BASEURL=http://127.0.0.1:%d", port),
		"MIKROTIK_USERNAME=admin",
		"MIKROTIK_PASSWORD="+testPassword,
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, 0, err
	}
	if err := cmd.Start(); err != nil {
		return nil, 0, err
	}
	results := make(map[string]string)
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		var ev testEvent
		if json.Unmarshal(sc.Bytes(), &ev) == nil && ev.Test != "" {
			switch ev.Action {
			case "pass", "fail", "skip":
				results[ev.Test] = ev.Action
			}
		}
	}
	_ = cmd.Wait()
	return results, time.Since(start).Seconds(), nil
}

// ── Per-version orchestration ──────────────────────────────────────────────

type versionResult struct {
	Version  string            `json:"version"`
	Tests    map[string]string `json:"tests"`
	BootSecs float64           `json:"boot_secs,omitempty"`
	TestSecs float64           `json:"test_secs,omitempty"`
	Passed   bool              `json:"passed"`
	Error    string            `json:"error,omitempty"`
}

func testVersion(ctx context.Context, version, cacheDir, projectDir string, port int, bootTimeout time.Duration) versionResult {
	logf := func(msg string, args ...any) {
		fmt.Fprintf(os.Stderr, "[%-14s] "+msg+"\n", append([]any{version}, args...)...)
	}
	res := versionResult{Version: version, Tests: map[string]string{}}

	logf("downloading")
	imgPath, err := ensureImage(ctx, version, cacheDir)
	if err != nil {
		res.Error = "download: " + err.Error()
		logf("FAILED %s", res.Error)
		return res
	}

	logf("booting on :%d", port)
	pidFile, err := bootVM(imgPath, port)
	if err != nil {
		res.Error = "boot: " + err.Error()
		logf("FAILED %s", res.Error)
		return res
	}
	defer killVM(pidFile)

	vmCtx, cancel := context.WithTimeout(ctx, bootTimeout)
	defer cancel()

	logf("waiting for API")
	bootSecs, err := waitReady(vmCtx, port)
	if err != nil {
		res.Error = "wait: " + err.Error()
		logf("FAILED %s", res.Error)
		return res
	}
	res.BootSecs = bootSecs
	logf("API ready in %.1fs", bootSecs)

	if err := setPassword(vmCtx, port); err != nil {
		res.Error = "setpass: " + err.Error()
		logf("FAILED %s", res.Error)
		return res
	}

	logf("running tests")
	tests, testSecs, err := runTests(ctx, port, projectDir)
	if err != nil {
		res.Error = "tests: " + err.Error()
		logf("FAILED %s", res.Error)
		return res
	}
	res.Tests = tests
	res.TestSecs = testSecs
	for _, s := range tests {
		if s == "fail" {
			logf("TESTS FAILED (%.1fs)", testSecs)
			return res
		}
	}
	res.Passed = true
	logf("passed (boot=%.1fs tests=%.1fs)", bootSecs, testSecs)
	return res
}

// ── Report generation ──────────────────────────────────────────────────────

var integrationTests = []string{
	"TestIntegration_Records_Empty",
	"TestIntegration_Create_A",
	"TestIntegration_Create_AAAA",
	"TestIntegration_Create_CNAME",
	"TestIntegration_Create_TXT",
	"TestIntegration_Create_MX",
	"TestIntegration_Create_SRV",
	"TestIntegration_Create_NS",
	"TestIntegration_Delete",
	"TestIntegration_Update",
	"TestIntegration_Update_MultipleTargets",
}

func shortName(t string) string { return strings.TrimPrefix(t, "TestIntegration_") }

func icon(s string) string {
	switch s {
	case "pass":
		return "✓"
	case "fail":
		return "✗"
	case "skip":
		return "–"
	default:
		return "?"
	}
}

func generateMarkdown(results []versionResult) string {
	var sb strings.Builder
	sb.WriteString("# RouterOS 7.x REST API Compatibility Matrix\n\n")
	fmt.Fprintf(&sb, "_Generated %s — tested against `external-dns-provider-mikrotik`_\n\n", time.Now().Format("2006-01-02"))

	// Header
	sb.WriteString("| Version | Overall |")
	for _, t := range integrationTests {
		sb.WriteString(" " + shortName(t) + " |")
	}
	sb.WriteString("\n|---------|---------|")
	for range integrationTests {
		sb.WriteString("---------|")
	}
	sb.WriteString("\n")

	passed, total := 0, 0
	for _, r := range results {
		total++
		overall := "✓"
		if r.Error != "" || !r.Passed {
			overall = "✗"
		} else {
			passed++
		}
		fmt.Fprintf(&sb, "| %-12s | %-7s |", r.Version, overall)
		for _, t := range integrationTests {
			fmt.Fprintf(&sb, " %-7s |", icon(r.Tests[t]))
		}
		if r.Error != "" {
			fmt.Fprintf(&sb, " <!-- %s -->", r.Error)
		}
		sb.WriteString("\n")
	}

	sb.WriteString("\n✓ pass  ✗ fail  – skip  ? not run\n")
	fmt.Fprintf(&sb, "\n**%d / %d versions fully passed**\n", passed, total)
	return sb.String()
}

// ── Main ───────────────────────────────────────────────────────────────────

func main() {
	cacheDir := flag.String("cache-dir", filepath.Join(os.TempDir(), "chr-compat-cache"), "directory for cached CHR images")
	parallel := flag.Int("parallel", 4, "concurrent VM tests")
	reportMD := flag.String("report", "compat-report.md", "markdown report output file")
	reportJSON := flag.String("json", "compat-report.json", "JSON report output file")
	versionsFlag := flag.String("versions", "", "comma-separated versions to test (skips discovery)")
	discoveryWorkers := flag.Int("discovery-workers", 50, "concurrency for version discovery")
	bootTimeout := flag.Duration("boot-timeout", 3*time.Minute, "max time to wait for RouterOS API per VM")
	flag.Parse()

	projectDir, _ := os.Getwd()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := os.MkdirAll(*cacheDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "cache dir: %v\n", err)
		os.Exit(1)
	}

	var versions []string
	if *versionsFlag != "" {
		for _, v := range strings.Split(*versionsFlag, ",") {
			if v = strings.TrimSpace(v); v != "" {
				versions = append(versions, v)
			}
		}
		fmt.Fprintf(os.Stderr, "Testing %d specified versions\n", len(versions))
	} else {
		fmt.Fprintf(os.Stderr, "Probing Mikrotik download server for all RouterOS 7.x CHR images...\n")
		versions = discoverVersions(ctx, *discoveryWorkers)
		fmt.Fprintf(os.Stderr, "Found %d versions: %s\n\n", len(versions), strings.Join(versions, ", "))
	}

	if len(versions) == 0 {
		fmt.Fprintln(os.Stderr, "no versions found")
		os.Exit(1)
	}

	// Port pool: one slot per parallel worker.
	ports := make(chan int, *parallel)
	for i := 0; i < *parallel; i++ {
		ports <- basePort + i*portStep
	}

	// Run tests; preserve input order in results slice.
	results := make([]versionResult, len(versions))
	var wg sync.WaitGroup
	for i, v := range versions {
		wg.Add(1)
		i, v := i, v
		go func() {
			defer wg.Done()
			port := <-ports
			defer func() { ports <- port }()
			results[i] = testVersion(ctx, v, *cacheDir, projectDir, port, *bootTimeout)
		}()
	}
	wg.Wait()

	// Write JSON report.
	jsonData, _ := json.MarshalIndent(results, "", "  ")
	if err := os.WriteFile(*reportJSON, jsonData, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "JSON report: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "\nJSON report written to %s\n", *reportJSON)
	}

	// Write and print markdown report.
	md := generateMarkdown(results)
	if err := os.WriteFile(*reportMD, []byte(md), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "markdown report: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "Markdown report written to %s\n", *reportMD)
	}
	fmt.Print(md)
}
