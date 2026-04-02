package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"

	"golang.org/x/sys/windows"
)

const (
	probeFsctlReadUSNJournal  = 0x000900bb
	probeFsctlQueryUSNJournal = 0x000900f4
)

type probeRequest struct {
	Volume      string `json:"volume"`
	WindowUSN   int64  `json:"window_usn"`
	OutBufBytes uint32 `json:"out_buf_bytes"`
	AllowC      bool   `json:"allow_c"`
}

type probeResponse struct {
	Volume           string         `json:"volume"`
	TotalRecords     int            `json:"total_records"`
	ByMajorVersion   map[uint16]int `json:"by_major_version"`
	ObservedVersions []uint16       `json:"observed_versions"`
	CurrentNextUSN   int64          `json:"current_next_usn"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:7790", "listen addr")
	defaultVolume := flag.String("volume", "D", "default volume letter, e.g. D")
	flag.Parse()

	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":             true,
			"default_volume": strings.ToUpper(strings.TrimSpace(*defaultVolume)),
		})
	})

	http.HandleFunc("/probe/usn-version", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		req := probeRequest{
			Volume:      *defaultVolume,
			WindowUSN:   8 * 1024 * 1024,
			OutBufBytes: 1024 * 1024,
			AllowC:      false,
		}
		if r.Body != nil {
			defer r.Body.Close()
			_ = json.NewDecoder(r.Body).Decode(&req)
		}
		if strings.TrimSpace(req.Volume) == "" {
			req.Volume = *defaultVolume
		}
		if req.WindowUSN <= 0 {
			req.WindowUSN = 8 * 1024 * 1024
		}
		if req.OutBufBytes < 64*1024 {
			req.OutBufBytes = 1024 * 1024
		}

		resp, err := probeUSNVersion(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	log.Printf("admin probe server listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func probeUSNVersion(req probeRequest) (probeResponse, error) {
	vol, err := normalizeVolume(req.Volume)
	if err != nil {
		return probeResponse{}, err
	}
	if vol == 'C' && !req.AllowC {
		return probeResponse{}, fmt.Errorf("refusing to read C: set allow_c=true to override")
	}

	h, err := openVolumeReadonlyProbe(vol)
	if err != nil {
		return probeResponse{}, err
	}
	defer windows.CloseHandle(h)

	jdRaw, err := deviceIoControlProbe(h, probeFsctlQueryUSNJournal, nil, 64)
	if err != nil {
		return probeResponse{}, fmt.Errorf("query journal failed: %w", err)
	}
	if len(jdRaw) < 24 {
		return probeResponse{}, fmt.Errorf("query journal output too small: %d", len(jdRaw))
	}
	journalID := binary.LittleEndian.Uint64(jdRaw[0:8])
	nextUSN := int64(binary.LittleEndian.Uint64(jdRaw[16:24]))

	startUSN := nextUSN - req.WindowUSN
	if startUSN < 0 {
		startUSN = 0
	}

	in := make([]byte, 40)
	binary.LittleEndian.PutUint64(in[0:8], uint64(startUSN))
	binary.LittleEndian.PutUint32(in[8:12], 0xffffffff)
	binary.LittleEndian.PutUint32(in[12:16], 0)
	binary.LittleEndian.PutUint64(in[16:24], 0)
	binary.LittleEndian.PutUint64(in[24:32], 0)
	binary.LittleEndian.PutUint64(in[32:40], journalID)
	// MinMajorVersion=2, MaxMajorVersion=4
	in = append(in, 2, 0, 4, 0)

	out, err := deviceIoControlProbe(h, probeFsctlReadUSNJournal, in, req.OutBufBytes)
	if err != nil {
		var errno windows.Errno
		if errors.As(err, &errno) {
			return probeResponse{}, fmt.Errorf("read journal failed (errno=%d)", uint32(errno))
		}
		return probeResponse{}, fmt.Errorf("read journal failed: %w", err)
	}
	if len(out) < 8 {
		return probeResponse{
			Volume:           string(vol),
			ByMajorVersion:   map[uint16]int{},
			ObservedVersions: []uint16{},
			CurrentNextUSN:   nextUSN,
		}, nil
	}

	counts := map[uint16]int{}
	total := 0
	pos := 8
	for pos+8 <= len(out) {
		recLen := int(binary.LittleEndian.Uint32(out[pos : pos+4]))
		if recLen < 8 || pos+recLen > len(out) {
			break
		}
		major := binary.LittleEndian.Uint16(out[pos+4 : pos+6])
		counts[major]++
		total++
		pos += recLen
	}

	versions := make([]uint16, 0, len(counts))
	for v := range counts {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })

	return probeResponse{
		Volume:           string(vol),
		TotalRecords:     total,
		ByMajorVersion:   counts,
		ObservedVersions: versions,
		CurrentNextUSN:   nextUSN,
	}, nil
}

func normalizeVolume(raw string) (rune, error) {
	s := strings.ToUpper(strings.TrimSpace(raw))
	s = strings.TrimSuffix(s, ":")
	if len(s) != 1 || s[0] < 'A' || s[0] > 'Z' {
		return 0, fmt.Errorf("invalid volume: %q", raw)
	}
	return rune(s[0]), nil
}

func openVolumeReadonlyProbe(volume rune) (windows.Handle, error) {
	path := fmt.Sprintf("\\\\.\\%c:", volume)
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	return windows.CreateFile(
		p,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
}

func deviceIoControlProbe(h windows.Handle, code uint32, in []byte, outSize uint32) ([]byte, error) {
	out := make([]byte, outSize)
	var returned uint32

	var inPtr *byte
	if len(in) > 0 {
		inPtr = &in[0]
	}
	var outPtr *byte
	if len(out) > 0 {
		outPtr = &out[0]
	}
	if err := windows.DeviceIoControl(h, code, inPtr, uint32(len(in)), outPtr, uint32(len(out)), &returned, nil); err != nil {
		return nil, err
	}
	return out[:returned], nil
}
