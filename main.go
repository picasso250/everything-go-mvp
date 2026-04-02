package main

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	"golang.org/x/sys/windows"
	_ "modernc.org/sqlite"
)

const (
	fsctlEnumUSNData     = 0x000900b3
	fsctlReadUSNJournal  = 0x000900bb
	fsctlQueryUSNJournal = 0x000900f4

	errorHandleEOF windows.Errno = 38

	fileAttributeDirectory = 0x10

	usnReasonFileDelete = 0x00000200
)

type journalData struct {
	USNJournalID    uint64
	FirstUSN        int64
	NextUSN         int64
	LowestValidUSN  int64
	MaxUSN          int64
	MaximumSize     uint64
	AllocationDelta uint64
}

type usnRecord struct {
	ID             string
	ParentID       string
	USN            int64
	Reason         uint32
	FileAttributes uint32
	Name           string
}

type daemonState struct {
	mu              sync.Mutex
	db              *sql.DB
	volume          rune
	deltaLog        bool
	currentUSN      int64
	journalID       uint64
	journalNextUSN  int64
	pendingLatest   map[string]usnRecord
	lastFlushAt     time.Time
	rebuildRequired bool
	rebuildReason   string
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: everything-go-mvp <anchor|full-build|rebuild|serve|search>")
		os.Exit(2)
	}

	switch os.Args[1] {
	case "anchor":
		if err := cmdAnchor(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "full-build":
		if err := cmdFullBuild(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "rebuild":
		if err := cmdRebuild(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "serve":
		if err := cmdServe(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "search":
		if err := cmdSearch(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	default:
		fmt.Println("unknown command, use anchor|full-build|rebuild|serve|search")
		os.Exit(2)
	}
}

func cmdAnchor(args []string) error {
	fs := flag.NewFlagSet("anchor", flag.ContinueOnError)
	volumesRaw := fs.String("volumes", "D", "comma-separated volume letters, example: D,E")
	dbPath := fs.String("db", "everything_mvp.db", "sqlite path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	volumes, err := parseVolumes(*volumesRaw)
	if err != nil {
		return err
	}

	db, err := openDB(*dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := initDB(db); err != nil {
		return err
	}

	for _, v := range volumes {
		h, err := openVolumeReadonly(v)
		if err != nil {
			return err
		}
		jd, err := queryUSNJournal(h)
		_ = windows.CloseHandle(h)
		if err != nil {
			return err
		}

		scope := string(v)
		if err := upsertMeta(db, scope, "volume", scope); err != nil {
			return err
		}
		if err := upsertMeta(db, scope, "anchor_journal_id", fmt.Sprintf("%d", jd.USNJournalID)); err != nil {
			return err
		}
		if err := upsertMeta(db, scope, "anchor_usn", fmt.Sprintf("%d", jd.NextUSN)); err != nil {
			return err
		}
		if _, err := getMeta(db, scope, "last_usn"); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if err := upsertMeta(db, scope, "last_usn", fmt.Sprintf("%d", jd.NextUSN)); err != nil {
					return err
				}
			} else {
				return err
			}
		}

		fmt.Printf("anchor saved: volume=%c journal_id=%d anchor_usn=%d db=%s\n", v, jd.USNJournalID, jd.NextUSN, *dbPath)
	}
	return nil
}

type snapshotEntry struct {
	ID             string
	ParentID       string
	Name           string
	IsDir          bool
	Path           string
	USN            int64
	Reason         uint32
	FileAttributes uint32
}

func cmdFullBuild(args []string) error {
	fs := flag.NewFlagSet("full-build", flag.ContinueOnError)
	volumesRaw := fs.String("volumes", "D", "comma-separated volume letters, example: D,E")
	dbPath := fs.String("db", "everything_mvp.db", "sqlite path")
	maxRecords := fs.Int("max-records", 50_000_000, "max records")
	chunkSize := fs.Int("chunk-size", 1024*1024, "DeviceIoControl output buffer")
	if err := fs.Parse(args); err != nil {
		return err
	}

	volumes, err := parseVolumes(*volumesRaw)
	if err != nil {
		return err
	}

	started := time.Now()
	db, err := openDB(*dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := initDB(db); err != nil {
		return err
	}

	var totalRaw, totalSnapshot, totalUnresolved int
	for _, v := range volumes {
		h, err := openVolumeReadonly(v)
		if err != nil {
			return err
		}
		jd, err := queryUSNJournal(h)
		if err != nil {
			_ = windows.CloseHandle(h)
			return err
		}

		records, err := enumUSNAll(h, 0, jd.NextUSN, *maxRecords, uint32(*chunkSize))
		_ = windows.CloseHandle(h)
		if err != nil {
			return err
		}

		for i := range records {
			records[i] = qualifyRecord(v, records[i])
		}
		entries, unresolved := buildSnapshotEntries(v, records)
		if err := writeFullSnapshot(db, v, jd, entries); err != nil {
			return err
		}

		totalRaw += len(records)
		totalSnapshot += len(entries)
		totalUnresolved += unresolved
		fmt.Printf("full-build volume=%c raw=%d snapshot=%d unresolved=%d\n", v, len(records), len(entries), unresolved)
	}

	fmt.Printf(
		"Go full-build -> SQLite complete: volumes=%d raw=%d snapshot=%d unresolved_parents=%d in %.2fs -> %s\n",
		len(volumes), totalRaw, totalSnapshot, totalUnresolved, time.Since(started).Seconds(), *dbPath,
	)
	return nil
}

func cmdRebuild(args []string) error {
	return cmdFullBuild(args)
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	volumesRaw := fs.String("volumes", "D", "comma-separated volume letters, example: D,E")
	dbPath := fs.String("db", "everything_mvp.db", "sqlite path")
	addr := fs.String("addr", "127.0.0.1:7788", "listen addr")
	flushSeconds := fs.Int("flush-seconds", 10, "flush interval")
	chunkSize := fs.Int("chunk-size", 1024*1024, "DeviceIoControl output buffer")
	deltaLog := fs.Bool("delta-log", false, "log every applied add/update/delete delta")
	if err := fs.Parse(args); err != nil {
		return err
	}

	volumes, err := parseVolumes(*volumesRaw)
	if err != nil {
		return err
	}

	db, err := openDB(*dbPath)
	if err != nil {
		return err
	}
	if err := initDB(db); err != nil {
		return err
	}

	states := make(map[rune]*daemonState, len(volumes))
	for _, v := range volumes {
		h, err := openVolumeReadonly(v)
		if err != nil {
			return err
		}
		jd, err := queryUSNJournal(h)
		if err != nil {
			_ = windows.CloseHandle(h)
			return err
		}

		scope := string(v)
		startUSN, err := resolveStartUSN(db, scope, jd)
		if err != nil {
			_ = markNeedsRebuild(db, scope, fmt.Sprintf("startup journal mismatch: %v", err))
			_ = windows.CloseHandle(h)
			return err
		}

		_ = clearNeedsRebuild(db, scope)
		if err := upsertMeta(db, scope, "usn_journal_id", fmt.Sprintf("%d", jd.USNJournalID)); err != nil {
			_ = windows.CloseHandle(h)
			return err
		}

		st := &daemonState{
			db:             db,
			volume:         v,
			deltaLog:       *deltaLog,
			currentUSN:     startUSN,
			journalID:      jd.USNJournalID,
			journalNextUSN: jd.NextUSN,
			pendingLatest:  map[string]usnRecord{},
		}
		states[v] = st

		go runVolumeLoop(st, h, uint32(*chunkSize), time.Duration(*flushSeconds)*time.Second)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		for _, st := range states {
			st.mu.Lock()
			lag := st.journalNextUSN - st.currentUSN
			if lag < 0 {
				lag = 0
			}
			lastFlush := ""
			if !st.lastFlushAt.IsZero() {
				lastFlush = st.lastFlushAt.Format(time.RFC3339)
			}
			rebuildRequired := st.rebuildRequired
			rebuildReason := st.rebuildReason
			current := st.currentUSN
			jid := st.journalID
			jnext := st.journalNextUSN
			platest := len(st.pendingLatest)
			st.mu.Unlock()
			lastUSN, _ := getMeta(st.db, string(st.volume), "last_usn")
			_, _ = fmt.Fprintf(
				w,
				"ok volume=%c journal_id=%d journal_next_usn=%d current_usn=%d last_usn=%s lag=%d pending_latest=%d last_flush_time=%s rebuild_required=%t rebuild_reason=%q\n",
				st.volume, jid, jnext, current, lastUSN, lag, platest, lastFlush, rebuildRequired, rebuildReason,
			)
		}
	})
	mux.HandleFunc("/flush", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		for _, st := range states {
			if err := flushPending(st); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = fmt.Fprintf(w, "flush failed: %v\n", err)
				return
			}
		}
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		for _, st := range states {
			st.mu.Lock()
			rebuildRequired := st.rebuildRequired
			rebuildReason := st.rebuildReason
			st.mu.Unlock()
			if rebuildRequired {
				w.WriteHeader(http.StatusConflict)
				_, _ = fmt.Fprintf(w, "rebuild required on volume %c: %s\n", st.volume, rebuildReason)
				return
			}
		}
		for _, st := range states {
			if err := flushPending(st); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = fmt.Fprintf(w, "flush failed: %v\n", err)
				return
			}
		}
		q := strings.TrimSpace(r.URL.Query().Get("q"))
		matchMode := normalizeMatchMode(r.URL.Query().Get("mode"))
		typeMode := normalizeTypeModeDefaultFile(r.URL.Query().Get("type"))
		limit := 50
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				limit = n
			}
		}
		paths, err := queryEntries(db, q, limit, matchMode, typeMode)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprintf(w, "query failed: %v\n", err)
			return
		}
		for _, p := range paths {
			_, _ = fmt.Fprintln(w, p)
		}
	})

	log.Printf("serve started: addr=%s volumes=%v", *addr, volumes)
	return http.ListenAndServe(*addr, mux)
}

func runVolumeLoop(st *daemonState, h windows.Handle, chunkSize uint32, flushEvery time.Duration) {
	defer windows.CloseHandle(h)
	if flushEvery <= 0 {
		flushEvery = 10 * time.Second
	}
	nextFlush := time.Now().Add(flushEvery)

	for {
		jlive, err := queryUSNJournal(h)
		if err != nil {
			log.Printf("volume %c query journal error: %v", st.volume, err)
			time.Sleep(1 * time.Second)
			continue
		}
		if jlive.USNJournalID != st.journalID {
			reason := fmt.Sprintf("journal changed: old=%d new=%d", st.journalID, jlive.USNJournalID)
			log.Printf("volume %c rebuild required: %s", st.volume, reason)
			st.mu.Lock()
			st.rebuildRequired = true
			st.rebuildReason = reason
			st.journalNextUSN = jlive.NextUSN
			st.mu.Unlock()
			_ = markNeedsRebuild(st.db, string(st.volume), reason)
			time.Sleep(1 * time.Second)
			continue
		}

		nextUSN, recs, err := readUSNBatch(h, st.currentUSN, st.journalID, chunkSize)
		if err != nil {
			if isRebuildRequiredUSNError(err) {
				reason := fmt.Sprintf("journal continuity lost: %v", err)
				log.Printf("volume %c rebuild required: %s", st.volume, reason)
				st.mu.Lock()
				st.rebuildRequired = true
				st.rebuildReason = reason
				st.mu.Unlock()
				_ = markNeedsRebuild(st.db, string(st.volume), reason)
				time.Sleep(1 * time.Second)
				continue
			}
			log.Printf("volume %c poll error: %v", st.volume, err)
			time.Sleep(1 * time.Second)
			continue
		}

		st.mu.Lock()
		st.currentUSN = nextUSN
		st.journalNextUSN = jlive.NextUSN
		for _, r := range recs {
			rq := qualifyRecord(st.volume, r)
			prev, ok := st.pendingLatest[rq.ID]
			if !ok || rq.USN >= prev.USN {
				st.pendingLatest[rq.ID] = rq
			}
		}
		st.mu.Unlock()

		if time.Now().After(nextFlush) {
			if err := flushPending(st); err != nil {
				log.Printf("volume %c flush error: %v", st.volume, err)
			}
			nextFlush = time.Now().Add(flushEvery)
		}
		time.Sleep(1 * time.Second)
	}
}

func cmdSearch(args []string) error {
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	addr := fs.String("addr", "http://127.0.0.1:7788", "daemon base url")
	query := fs.String("query", "", "query")
	limit := fs.Int("limit", 50, "limit")
	match := fs.String("match", "name", "match mode: name|path|all")
	typ := fs.String("type", "file", "type filter: file|dir|all")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*query) == "" {
		return fmt.Errorf("--query is required")
	}

	mode := normalizeMatchMode(*match)
	typeMode := normalizeTypeModeDefaultFile(*typ)
	u := fmt.Sprintf(
		"%s/search?q=%s&limit=%d&mode=%s&type=%s",
		strings.TrimRight(*addr, "/"),
		url.QueryEscape(*query),
		*limit,
		url.QueryEscape(mode),
		url.QueryEscape(typeMode),
	)
	resp, err := http.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("server error: %s", strings.TrimSpace(string(body)))
	}
	fmt.Print(string(body))
	return nil
}

func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func initDB(db *sql.DB) error {
	_, err := db.Exec(`
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS meta (
  scope TEXT NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY(scope, key)
);

CREATE TABLE IF NOT EXISTS entries (
  id TEXT PRIMARY KEY,
  parent_id TEXT NOT NULL,
  name TEXT NOT NULL,
  is_dir INTEGER NOT NULL,
  path TEXT NOT NULL,
  usn INTEGER NOT NULL,
  reason INTEGER NOT NULL,
  file_attributes INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_entries_path ON entries(path);
CREATE INDEX IF NOT EXISTS idx_entries_name ON entries(name);
`)
	return err
}

func enumUSNAll(h windows.Handle, lowUSN, highUSN int64, maxRecords int, chunkSize uint32) ([]usnRecord, error) {
	records := make([]usnRecord, 0, min(maxRecords, 8192))
	var startFRN uint64 = 0

	for len(records) < maxRecords {
		in := make([]byte, 28)
		binary.LittleEndian.PutUint64(in[0:8], startFRN)
		binary.LittleEndian.PutUint64(in[8:16], uint64(lowUSN))
		binary.LittleEndian.PutUint64(in[16:24], uint64(highUSN))
		binary.LittleEndian.PutUint16(in[24:26], 2) // min major
		binary.LittleEndian.PutUint16(in[26:28], 3) // max major

		out, err := deviceIoControl(h, fsctlEnumUSNData, in, chunkSize)
		if err != nil {
			if errors.Is(err, errorHandleEOF) {
				break
			}
			return nil, err
		}
		if len(out) < 8 {
			break
		}

		nextFRN := binary.LittleEndian.Uint64(out[0:8])
		pos := 8
		consumed := false
		for pos+60 <= len(out) && len(records) < maxRecords {
			recLen := int(binary.LittleEndian.Uint32(out[pos : pos+4]))
			if recLen < 60 || pos+recLen > len(out) {
				break
			}
			major := binary.LittleEndian.Uint16(out[pos+4 : pos+6])
			recBytes := out[pos : pos+recLen]
			if major == 2 {
				if r, ok := parseUSNRecordV2(recBytes); ok {
					records = append(records, r)
				}
			} else if major == 3 {
				if r, ok := parseUSNRecordV3(recBytes); ok {
					records = append(records, r)
				}
			}
			pos += recLen
			consumed = true
		}
		if !consumed || nextFRN == startFRN {
			break
		}
		startFRN = nextFRN
	}

	return records, nil
}

func buildSnapshotEntries(volume rune, records []usnRecord) ([]snapshotEntry, int) {
	byID := make(map[string]usnRecord, len(records))
	for _, r := range records {
		prev, ok := byID[r.ID]
		if !ok || preferRecordForSnapshot(r, prev) {
			byID[r.ID] = r
		}
	}

	memo := make(map[string]string, len(records))
	unresolved := 0
	entries := make([]snapshotEntry, 0, len(records))
	for _, r := range byID {
		if r.ParentID != r.ID {
			if _, ok := byID[r.ParentID]; !ok {
				unresolved++
			}
		}
		p := resolvePath(volume, r.ID, byID, memo, map[string]bool{})
		entries = append(entries, snapshotEntry{
			ID:             r.ID,
			ParentID:       r.ParentID,
			Name:           r.Name,
			IsDir:          (r.FileAttributes & fileAttributeDirectory) != 0,
			Path:           p,
			USN:            r.USN,
			Reason:         r.Reason,
			FileAttributes: r.FileAttributes,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return strings.ToLower(entries[i].Path) < strings.ToLower(entries[j].Path)
	})
	return entries, unresolved
}

func preferRecordForSnapshot(candidate, existing usnRecord) bool {
	cn := strings.TrimSpace(candidate.Name)
	en := strings.TrimSpace(existing.Name)

	// Prefer records that carry a usable name; this avoids collapsing to unnamed
	// entries when the same file ID appears multiple times.
	if cn != "" && en == "" {
		return true
	}
	if cn == "" && en != "" {
		return false
	}

	// If both have names, prefer the more informative one.
	if len(cn) != len(en) {
		return len(cn) > len(en)
	}

	// Last tie-breaker: newer USN.
	return candidate.USN >= existing.USN
}

func resolvePath(volume rune, id string, byID map[string]usnRecord, memo map[string]string, visiting map[string]bool) string {
	if p, ok := memo[id]; ok {
		return p
	}
	node, ok := byID[id]
	if !ok {
		return fmt.Sprintf("%c:\\<unknown:%s>", volume, id)
	}
	if visiting[id] {
		return fmt.Sprintf("%c:\\<loop>\\%s", volume, node.Name)
	}
	visiting[id] = true

	var out string
	if node.ParentID == node.ID {
		out = fmt.Sprintf("%c:\\%s", volume, node.Name)
	} else if _, ok := byID[node.ParentID]; ok {
		parent := resolvePath(volume, node.ParentID, byID, memo, visiting)
		out = strings.TrimRight(parent, "\\") + "\\" + node.Name
	} else {
		if node.Name == "" {
			out = fmt.Sprintf("%c:\\", volume)
		} else {
			out = fmt.Sprintf("%c:\\%s", volume, node.Name)
		}
	}
	delete(visiting, id)
	memo[id] = out
	return out
}

func writeFullSnapshot(db *sql.DB, volume rune, jd journalData, entries []snapshotEntry) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	prefix := fmt.Sprintf("%c|%%", volume)
	if _, err := tx.Exec(`DELETE FROM entries WHERE id LIKE ?`, prefix); err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
INSERT INTO entries(id,parent_id,name,is_dir,path,usn,reason,file_attributes)
VALUES(?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, e := range entries {
		isDir := 0
		if e.IsDir {
			isDir = 1
		}
		if _, err := stmt.Exec(e.ID, e.ParentID, e.Name, isDir, e.Path, e.USN, e.Reason, e.FileAttributes); err != nil {
			return err
		}
	}

	scope := string(volume)
	if err := upsertMetaTx(tx, scope, "volume", scope); err != nil {
		return err
	}
	if err := upsertMetaTx(tx, scope, "usn_journal_id", fmt.Sprintf("%d", jd.USNJournalID)); err != nil {
		return err
	}
	if err := upsertMetaTx(tx, scope, "first_usn", fmt.Sprintf("%d", jd.FirstUSN)); err != nil {
		return err
	}
	if err := upsertMetaTx(tx, scope, "next_usn", fmt.Sprintf("%d", jd.NextUSN)); err != nil {
		return err
	}
	if err := upsertMetaTx(tx, scope, "lowest_valid_usn", fmt.Sprintf("%d", jd.LowestValidUSN)); err != nil {
		return err
	}
	if err := upsertMetaTx(tx, scope, "max_usn", fmt.Sprintf("%d", jd.MaxUSN)); err != nil {
		return err
	}
	if err := upsertMetaTx(tx, scope, "maximum_size", fmt.Sprintf("%d", jd.MaximumSize)); err != nil {
		return err
	}
	if err := upsertMetaTx(tx, scope, "allocation_delta", fmt.Sprintf("%d", jd.AllocationDelta)); err != nil {
		return err
	}
	if err := upsertMetaTx(tx, scope, "last_usn", fmt.Sprintf("%d", jd.NextUSN)); err != nil {
		return err
	}
	if err := upsertMetaTx(tx, scope, "anchor_journal_id", fmt.Sprintf("%d", jd.USNJournalID)); err != nil {
		return err
	}
	if err := upsertMetaTx(tx, scope, "anchor_usn", fmt.Sprintf("%d", jd.NextUSN)); err != nil {
		return err
	}
	if err := upsertMetaTx(tx, scope, "needs_rebuild", "0"); err != nil {
		return err
	}
	if err := upsertMetaTx(tx, scope, "needs_rebuild_reason", ""); err != nil {
		return err
	}

	return tx.Commit()
}

func flushPending(st *daemonState) error {
	st.mu.Lock()
	if len(st.pendingLatest) == 0 {
		curr := st.currentUSN
		st.mu.Unlock()
		return upsertMeta(st.db, string(st.volume), "last_usn", fmt.Sprintf("%d", curr))
	}
	latest := make([]usnRecord, 0, len(st.pendingLatest))
	for _, v := range st.pendingLatest {
		latest = append(latest, v)
	}
	curr := st.currentUSN
	st.pendingLatest = map[string]usnRecord{}
	st.mu.Unlock()

	tx, err := st.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, r := range latest {
		res, err := applyLatestRecord(tx, st.volume, r)
		if err != nil {
			return err
		}
		if st.deltaLog {
			switch res.Action {
			case "add":
				log.Printf("delta volume=%c action=add id=%s name=%q path=%q reason=0x%08x attrs=0x%08x", st.volume, r.ID, r.Name, res.NewPath, r.Reason, r.FileAttributes)
			case "delete":
				log.Printf("delta volume=%c action=delete id=%s name=%q path=%q reason=0x%08x attrs=0x%08x", st.volume, r.ID, r.Name, res.OldPath, r.Reason, r.FileAttributes)
			case "update":
				log.Printf("delta volume=%c action=update id=%s name=%q old_path=%q new_path=%q reason=0x%08x attrs=0x%08x", st.volume, r.ID, r.Name, res.OldPath, res.NewPath, r.Reason, r.FileAttributes)
			case "dir_move":
				log.Printf("delta volume=%c action=dir_move id=%s name=%q old_path=%q new_path=%q descendants_updated=%d reason=0x%08x attrs=0x%08x", st.volume, r.ID, r.Name, res.OldPath, res.NewPath, res.SubtreeUpdated, r.Reason, r.FileAttributes)
			}
		}
	}

	if err := upsertMetaTx(tx, string(st.volume), "last_usn", fmt.Sprintf("%d", curr)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	st.mu.Lock()
	st.lastFlushAt = time.Now()
	st.mu.Unlock()
	return nil
}

type applyResult struct {
	Action         string
	OldPath        string
	NewPath        string
	SubtreeUpdated int64
}

func applyLatestRecord(tx *sql.Tx, volume rune, r usnRecord) (applyResult, error) {
	var out applyResult
	if (r.Reason & usnReasonFileDelete) != 0 {
		existingPath := ""
		_ = tx.QueryRow(`SELECT path FROM entries WHERE id=? LIMIT 1`, r.ID).Scan(&existingPath)
		_, err := tx.Exec(`DELETE FROM entries WHERE id=?`, r.ID)
		if err != nil {
			return out, err
		}
		if existingPath != "" {
			out.Action = "delete"
			out.OldPath = existingPath
		}
		return out, nil
	}

	parentPath := ""
	_ = tx.QueryRow(`SELECT path FROM entries WHERE id=? LIMIT 1`, r.ParentID).Scan(&parentPath)
	existingPath := ""
	_ = tx.QueryRow(`SELECT path FROM entries WHERE id=? LIMIT 1`, r.ID).Scan(&existingPath)

	path := derivePath(volume, parentPath, existingPath, r.Name)
	isDir := 0
	if (r.FileAttributes & fileAttributeDirectory) != 0 {
		isDir = 1
	}

	_, err := tx.Exec(`
INSERT INTO entries(id,parent_id,name,is_dir,path,usn,reason,file_attributes)
VALUES(?,?,?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET
  parent_id=excluded.parent_id,
  name=excluded.name,
  is_dir=excluded.is_dir,
  path=excluded.path,
  usn=excluded.usn,
  reason=excluded.reason,
  file_attributes=excluded.file_attributes
`, r.ID, r.ParentID, r.Name, isDir, path, r.USN, r.Reason, r.FileAttributes)
	if err != nil {
		return out, err
	}

	if existingPath == "" {
		out.Action = "add"
		out.NewPath = path
	} else {
		out.Action = "update"
		out.OldPath = existingPath
		out.NewPath = path
	}

	// If a directory moved/renamed, rewrite descendant cached paths in-place.
	if isDir == 1 && existingPath != "" && !strings.EqualFold(existingPath, path) {
		oldPrefix := strings.TrimRight(existingPath, "\\") + "\\"
		newPrefix := strings.TrimRight(path, "\\") + "\\"
		if !strings.EqualFold(oldPrefix, newPrefix) {
			res, err := tx.Exec(
				`UPDATE entries SET path = ? || substr(path, ?) WHERE path LIKE ? COLLATE NOCASE`,
				newPrefix,
				len(oldPrefix)+1,
				oldPrefix+"%",
			)
			if err != nil {
				return out, err
			}
			updatedRows, _ := res.RowsAffected()
			out.Action = "dir_move"
			out.SubtreeUpdated = updatedRows
		}
	}
	return out, nil
}

func derivePath(volume rune, parentPath, existingPath, name string) string {
	if parentPath != "" {
		return strings.TrimRight(parentPath, "\\") + "\\" + name
	}
	if existingPath != "" {
		i := strings.LastIndex(existingPath, "\\")
		if i >= 0 {
			return existingPath[:i+1] + name
		}
	}
	return fmt.Sprintf("%c:\\%s", volume, name)
}

func queryEntries(db *sql.DB, query string, limit int, matchMode, typeMode string) ([]string, error) {
	terms := strings.Fields(strings.TrimSpace(query))
	if limit <= 0 {
		limit = 50
	}
	mode := normalizeMatchMode(matchMode)
	typ := normalizeTypeModeDefaultFile(typeMode)

	sqlStr := `SELECT path FROM entries`
	args := make([]any, 0, len(terms)*2+1)
	clauses := make([]string, 0, len(terms)+1)

	if typ == "file" {
		clauses = append(clauses, "is_dir = 0")
	} else if typ == "dir" {
		clauses = append(clauses, "is_dir = 1")
	}

	if len(terms) > 0 {
		for _, term := range terms {
			pat := "%" + term + "%"
			switch mode {
			case "path":
				clauses = append(clauses, `(path LIKE ? COLLATE NOCASE)`)
				args = append(args, pat)
			case "all":
				clauses = append(clauses, `(name LIKE ? COLLATE NOCASE OR path LIKE ? COLLATE NOCASE)`)
				args = append(args, pat, pat)
			default:
				clauses = append(clauses, `(name LIKE ? COLLATE NOCASE)`)
				args = append(args, pat)
			}
		}
	}
	if len(clauses) > 0 {
		sqlStr += " WHERE " + strings.Join(clauses, " AND ")
	}
	sqlStr += " LIMIT ?"
	args = append(args, limit)

	rows, err := db.Query(sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	hits := make([]string, 0, limit)
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		hits = append(hits, path)
	}
	return hits, rows.Err()
}

func normalizeMatchMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "path":
		return "path"
	case "all":
		return "all"
	default:
		return "name"
	}
}

func normalizeTypeModeDefaultFile(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "file":
		return "file"
	case "dir":
		return "dir"
	case "all":
		return "all"
	default:
		return "file"
	}
}

func resolveStartUSN(db *sql.DB, scope string, jd journalData) (int64, error) {
	if jIDStr, err := getMeta(db, scope, "anchor_journal_id"); err == nil {
		if jIDStr != fmt.Sprintf("%d", jd.USNJournalID) {
			return 0, fmt.Errorf("journal_id changed (anchor=%s current=%d), rebuild required", jIDStr, jd.USNJournalID)
		}
	}
	if v, err := getMeta(db, scope, "last_usn"); err == nil {
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return n, nil
		}
	}
	if v, err := getMeta(db, scope, "anchor_usn"); err == nil {
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return n, nil
		}
	}
	return jd.NextUSN, nil
}

func upsertMeta(db *sql.DB, scope, key, value string) error {
	_, err := db.Exec(`INSERT INTO meta(scope,key,value) VALUES(?,?,?) ON CONFLICT(scope,key) DO UPDATE SET value=excluded.value`, scope, key, value)
	return err
}

func upsertMetaTx(tx *sql.Tx, scope, key, value string) error {
	_, err := tx.Exec(`INSERT INTO meta(scope,key,value) VALUES(?,?,?) ON CONFLICT(scope,key) DO UPDATE SET value=excluded.value`, scope, key, value)
	return err
}

func getMeta(db *sql.DB, scope, key string) (string, error) {
	var v string
	err := db.QueryRow(`SELECT value FROM meta WHERE scope=? AND key=? LIMIT 1`, scope, key).Scan(&v)
	return v, err
}

func parseVolumes(raw string) ([]rune, error) {
	parts := strings.Split(raw, ",")
	seen := map[rune]bool{}
	out := make([]rune, 0, len(parts))
	for _, p := range parts {
		v, err := normalizeVolume(strings.TrimSpace(p))
		if err != nil {
			return nil, err
		}
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no valid volumes provided")
	}
	return out, nil
}

func qualifyRecord(volume rune, r usnRecord) usnRecord {
	q := r
	q.ID = fmt.Sprintf("%c|%s", volume, r.ID)
	q.ParentID = fmt.Sprintf("%c|%s", volume, r.ParentID)
	return q
}

func markNeedsRebuild(db *sql.DB, scope, reason string) error {
	if err := upsertMeta(db, scope, "needs_rebuild", "1"); err != nil {
		return err
	}
	if err := upsertMeta(db, scope, "needs_rebuild_reason", reason); err != nil {
		return err
	}
	return nil
}

func clearNeedsRebuild(db *sql.DB, scope string) error {
	if err := upsertMeta(db, scope, "needs_rebuild", "0"); err != nil {
		return err
	}
	if err := upsertMeta(db, scope, "needs_rebuild_reason", ""); err != nil {
		return err
	}
	return nil
}

func normalizeVolume(raw string) (rune, error) {
	s := strings.ToUpper(strings.TrimSpace(raw))
	s = strings.TrimSuffix(s, ":")
	if len(s) != 1 || s[0] < 'A' || s[0] > 'Z' {
		return 0, fmt.Errorf("invalid volume: %s", raw)
	}
	return rune(s[0]), nil
}

func guardSystemVolume(v rune, allow bool) error {
	if v == 'C' && !allow {
		return fmt.Errorf("refusing to read system volume C: without --allow-system-volume")
	}
	return nil
}

func openVolumeReadonly(volume rune) (windows.Handle, error) {
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

func queryUSNJournal(h windows.Handle) (journalData, error) {
	out, err := deviceIoControl(h, fsctlQueryUSNJournal, nil, 64)
	if err != nil {
		return journalData{}, err
	}
	if len(out) < 56 {
		return journalData{}, fmt.Errorf("query journal buffer too small")
	}
	jd := journalData{
		USNJournalID:    binary.LittleEndian.Uint64(out[0:8]),
		FirstUSN:        int64(binary.LittleEndian.Uint64(out[8:16])),
		NextUSN:         int64(binary.LittleEndian.Uint64(out[16:24])),
		LowestValidUSN:  int64(binary.LittleEndian.Uint64(out[24:32])),
		MaxUSN:          int64(binary.LittleEndian.Uint64(out[32:40])),
		MaximumSize:     binary.LittleEndian.Uint64(out[40:48]),
		AllocationDelta: binary.LittleEndian.Uint64(out[48:56]),
	}
	return jd, nil
}

func readUSNBatch(h windows.Handle, startUSN int64, journalID uint64, chunkSize uint32) (int64, []usnRecord, error) {
	in := make([]byte, 40)
	binary.LittleEndian.PutUint64(in[0:8], uint64(startUSN))
	binary.LittleEndian.PutUint32(in[8:12], 0xffffffff)
	binary.LittleEndian.PutUint32(in[12:16], 0)
	binary.LittleEndian.PutUint64(in[16:24], 0)
	binary.LittleEndian.PutUint64(in[24:32], 0)
	binary.LittleEndian.PutUint64(in[32:40], journalID)
	// min/max major version for V1 is trailing two uint16; extend buffer to 44.
	in = append(in, 2, 0, 3, 0)

	out, err := deviceIoControl(h, fsctlReadUSNJournal, in, chunkSize)
	if err != nil {
		if errors.Is(err, errorHandleEOF) {
			return startUSN, nil, nil
		}
		return startUSN, nil, err
	}
	if len(out) < 8 {
		return startUSN, nil, nil
	}

	nextUSN := int64(binary.LittleEndian.Uint64(out[0:8]))
	pos := 8
	recs := make([]usnRecord, 0, 128)
	for pos+60 <= len(out) {
		recLen := int(binary.LittleEndian.Uint32(out[pos : pos+4]))
		if recLen < 60 || pos+recLen > len(out) {
			break
		}
		major := binary.LittleEndian.Uint16(out[pos+4 : pos+6])
		recBytes := out[pos : pos+recLen]
		if major == 2 {
			if r, ok := parseUSNRecordV2(recBytes); ok {
				recs = append(recs, r)
			}
		} else if major == 3 {
			if r, ok := parseUSNRecordV3(recBytes); ok {
				recs = append(recs, r)
			}
		}
		pos += recLen
	}
	return nextUSN, recs, nil
}

func parseUSNRecordV2(rec []byte) (usnRecord, bool) {
	if len(rec) < 60 {
		return usnRecord{}, false
	}
	id := fmt.Sprintf("0x%016x", binary.LittleEndian.Uint64(rec[8:16]))
	parent := fmt.Sprintf("0x%016x", binary.LittleEndian.Uint64(rec[16:24]))
	usn := int64(binary.LittleEndian.Uint64(rec[24:32]))
	reason := binary.LittleEndian.Uint32(rec[40:44])
	attrs := binary.LittleEndian.Uint32(rec[52:56])
	nameLen := int(binary.LittleEndian.Uint16(rec[56:58]))
	nameOff := int(binary.LittleEndian.Uint16(rec[58:60]))
	if nameLen%2 != 0 || nameOff+nameLen > len(rec) {
		return usnRecord{}, false
	}
	name := decodeUTF16LE(rec[nameOff : nameOff+nameLen])
	return usnRecord{ID: id, ParentID: parent, USN: usn, Reason: reason, FileAttributes: attrs, Name: name}, true
}

func parseUSNRecordV3(rec []byte) (usnRecord, bool) {
	if len(rec) < 76 {
		return usnRecord{}, false
	}
	id := "0x" + hexLower(rec[8:24])
	parent := "0x" + hexLower(rec[24:40])
	usn := int64(binary.LittleEndian.Uint64(rec[40:48]))
	reason := binary.LittleEndian.Uint32(rec[56:60])
	attrs := binary.LittleEndian.Uint32(rec[68:72])
	nameLen := int(binary.LittleEndian.Uint16(rec[72:74]))
	nameOff := int(binary.LittleEndian.Uint16(rec[74:76]))
	if nameLen%2 != 0 || nameOff+nameLen > len(rec) {
		return usnRecord{}, false
	}
	name := decodeUTF16LE(rec[nameOff : nameOff+nameLen])
	return usnRecord{ID: id, ParentID: parent, USN: usn, Reason: reason, FileAttributes: attrs, Name: name}, true
}

func decodeUTF16LE(b []byte) string {
	u := make([]uint16, 0, len(b)/2)
	for i := 0; i+1 < len(b); i += 2 {
		u = append(u, binary.LittleEndian.Uint16(b[i:i+2]))
	}
	return string(utf16.Decode(u))
}

func hexLower(b []byte) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hexdigits[v>>4]
		out[i*2+1] = hexdigits[v&0x0f]
	}
	return string(out)
}

func deviceIoControl(h windows.Handle, code uint32, in []byte, outSize uint32) ([]byte, error) {
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

	err := windows.DeviceIoControl(h, code, inPtr, uint32(len(in)), outPtr, uint32(len(out)), &returned, nil)
	if err != nil {
		if errno, ok := err.(windows.Errno); ok {
			return nil, errno
		}
		return nil, err
	}
	return out[:returned], nil
}

func isRebuildRequiredUSNError(err error) bool {
	var errno windows.Errno
	if !errors.As(err, &errno) {
		return false
	}
	return errno == windows.ERROR_JOURNAL_DELETE_IN_PROGRESS ||
		errno == windows.ERROR_JOURNAL_NOT_ACTIVE ||
		errno == windows.ERROR_JOURNAL_ENTRY_DELETED
}
