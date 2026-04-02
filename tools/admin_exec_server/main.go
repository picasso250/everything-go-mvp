package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type runRequest struct {
	Command    string   `json:"command"`
	Args       []string `json:"args"`
	Cwd        string   `json:"cwd"`
	TimeoutSec int      `json:"timeout_sec"`
	Token      string   `json:"token"`
}

type runResponse struct {
	OK         bool   `json:"ok"`
	ExitCode   int    `json:"exit_code"`
	DurationMS int64  `json:"duration_ms"`
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	Error      string `json:"error,omitempty"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:7791", "listen addr (use loopback only)")
	baseDir := flag.String("base-dir", "", "allowed base dir for cwd and relative command paths")
	token := flag.String("token", "", "optional shared token required by /run")
	maxTimeout := flag.Int("max-timeout-sec", 180, "max timeout seconds for one command")
	flag.Parse()

	if *baseDir == "" {
		wd, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		baseDir = &wd
	}
	absBase, err := filepath.Abs(*baseDir)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"base_dir": absBase,
		})
	})

	mux.HandleFunc("/run", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req runRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, runResponse{OK: false, ExitCode: -1, Error: "invalid json"})
			return
		}
		if req.Command == "" {
			writeJSON(w, http.StatusBadRequest, runResponse{OK: false, ExitCode: -1, Error: "command is required"})
			return
		}
		if *token != "" && req.Token != *token {
			writeJSON(w, http.StatusForbidden, runResponse{OK: false, ExitCode: -1, Error: "invalid token"})
			return
		}

		cwd := absBase
		if strings.TrimSpace(req.Cwd) != "" {
			p, err := filepath.Abs(req.Cwd)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, runResponse{OK: false, ExitCode: -1, Error: "invalid cwd"})
				return
			}
			if !isSubPath(p, absBase) {
				writeJSON(w, http.StatusBadRequest, runResponse{OK: false, ExitCode: -1, Error: "cwd outside base-dir"})
				return
			}
			cwd = p
		}

		cmdPath, err := resolveCommand(req.Command, absBase, cwd)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, runResponse{OK: false, ExitCode: -1, Error: err.Error()})
			return
		}

		timeout := req.TimeoutSec
		if timeout <= 0 {
			timeout = 60
		}
		if timeout > *maxTimeout {
			timeout = *maxTimeout
		}

		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, cmdPath, req.Args...)
		cmd.Dir = cwd
		var outBuf, errBuf bytes.Buffer
		cmd.Stdout = &outBuf
		cmd.Stderr = &errBuf
		runErr := cmd.Run()
		dur := time.Since(start).Milliseconds()

		resp := runResponse{
			OK:         runErr == nil,
			ExitCode:   0,
			DurationMS: dur,
			Stdout:     trimOutput(outBuf.String()),
			Stderr:     trimOutput(errBuf.String()),
		}
		if runErr != nil {
			resp.OK = false
			resp.Error = runErr.Error()
			resp.ExitCode = exitCode(runErr)
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				resp.Error = fmt.Sprintf("timeout after %ds", timeout)
			}
			writeJSON(w, http.StatusOK, resp)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})

	log.Printf("admin exec server listening on %s (base-dir=%s)", *addr, absBase)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func resolveCommand(raw, absBase, cwd string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty command")
	}

	// Allow PATH commands for go/cmd/powershell only.
	switch strings.ToLower(raw) {
	case "go", "cmd", "powershell", "pwsh":
		return raw, nil
	}

	// Allow absolute exe path only under base-dir.
	if filepath.IsAbs(raw) {
		abs := filepath.Clean(raw)
		if !isSubPath(abs, absBase) {
			return "", fmt.Errorf("absolute command path outside base-dir")
		}
		return abs, nil
	}

	// Allow relative path command under cwd/base-dir, mostly for .\\xxx.exe.
	joined := filepath.Clean(filepath.Join(cwd, raw))
	if !isSubPath(joined, absBase) {
		return "", fmt.Errorf("relative command path outside base-dir")
	}
	return joined, nil
}

func isSubPath(path, base string) bool {
	p := strings.ToLower(filepath.Clean(path))
	b := strings.ToLower(filepath.Clean(base))
	if p == b {
		return true
	}
	if strings.HasSuffix(b, string(filepath.Separator)) {
		return strings.HasPrefix(p, b)
	}
	return strings.HasPrefix(p, b+string(filepath.Separator))
}

func trimOutput(s string) string {
	const max = 200000
	if len(s) <= max {
		return s
	}
	return s[:max] + "\n...[truncated]..."
}

func exitCode(err error) int {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return -1
}
