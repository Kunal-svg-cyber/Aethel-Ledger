package tlsconfig

import (
	"crypto/tls"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestLoadRequiresBothFiles(t *testing.T) {
	if _, err := Load("", ""); err == nil {
		t.Fatal("expected error")
	}
	if _, err := Load("a.pem", ""); err == nil {
		t.Fatal("expected error")
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load("nope.pem", "nope.key"); err == nil {
		t.Fatal("expected error")
	}
}

func TestLoadGeneratedCert(t *testing.T) {
	dir := t.TempDir()
	gen := filepath.Join(dir, "gen")
	build := exec.Command("go", "build", "-o", gen, "../../cmd/gencert")
	if out, err := build.CombinedOutput(); err != nil {
		t.Skipf("cannot build gencert: %v\n%s", err, out)
	}
	run := exec.Command(gen)
	run.Dir = dir
	if out, err := run.CombinedOutput(); err != nil {
		t.Fatalf("gencert: %v\n%s", err, out)
	}
	cfg, err := Load(filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("MinVersion = %x", cfg.MinVersion)
	}
	if _, err := os.Stat(filepath.Join(dir, "key.pem")); err != nil {
		t.Fatal(err)
	}
}
