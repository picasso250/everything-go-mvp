package main

import (
	"database/sql"
	"encoding/binary"
	"testing"

	_ "modernc.org/sqlite"
)

func TestParseUSNRecordV3Offsets(t *testing.T) {
	// Build a minimal synthetic V3 record with known values.
	name := "abc.txt"
	nameBytes := utf16LE(name)
	recLen := 76 + len(nameBytes)
	rec := make([]byte, recLen)

	binary.LittleEndian.PutUint32(rec[0:4], uint32(recLen))
	binary.LittleEndian.PutUint16(rec[4:6], 3) // major
	binary.LittleEndian.PutUint16(rec[6:8], 0) // minor

	fileID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	parentID := []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20}
	copy(rec[8:24], fileID)
	copy(rec[24:40], parentID)

	binary.LittleEndian.PutUint64(rec[40:48], uint64(123456789)) // usn
	// [48:56] is timestamp, leave as 0
	binary.LittleEndian.PutUint32(rec[56:60], 0x00002000) // reason
	// [60:68] source/security, leave as 0
	binary.LittleEndian.PutUint32(rec[68:72], 0x00000020) // file attrs
	binary.LittleEndian.PutUint16(rec[72:74], uint16(len(nameBytes)))
	binary.LittleEndian.PutUint16(rec[74:76], 76)
	copy(rec[76:], nameBytes)

	got, ok := parseUSNRecordV3(rec)
	if !ok {
		t.Fatalf("parseUSNRecordV3 returned !ok")
	}
	if got.Reason != 0x00002000 {
		t.Fatalf("reason mismatch: got=0x%08x", got.Reason)
	}
	if got.FileAttributes != 0x00000020 {
		t.Fatalf("file attrs mismatch: got=0x%08x", got.FileAttributes)
	}
	if got.Name != name {
		t.Fatalf("name mismatch: got=%q want=%q", got.Name, name)
	}
	if got.USN != 123456789 {
		t.Fatalf("usn mismatch: got=%d", got.USN)
	}
}

func TestApplyLatestRecordDirMoveCascadesDescendants(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := initDB(db); err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Seed:
	// D:\root          (D|root)
	// D:\root\old      (D|old)
	// D:\root\old\a.txt (D|file)
	_, err = tx.Exec(`
INSERT INTO entries(id,parent_id,name,is_dir,path,usn,reason,file_attributes) VALUES
('D|root','D|root','root',1,'D:\root',1,0,16),
('D|old','D|root','old',1,'D:\root\old',2,0,16),
('D|file','D|old','a.txt',0,'D:\root\old\a.txt',3,0,32)
`)
	if err != nil {
		t.Fatal(err)
	}

	res, err := applyLatestRecord(tx, 'D', usnRecord{
		ID:             "D|old",
		ParentID:       "D|root",
		USN:            10,
		Reason:         0x00002000,
		FileAttributes: 16,
		Name:           "new",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Action != "dir_move" {
		t.Fatalf("expected dir_move, got %q", res.Action)
	}
	if res.NewPath != `D:\root\new` {
		t.Fatalf("new path mismatch: got %q", res.NewPath)
	}

	var childPath string
	if err := tx.QueryRow(`SELECT path FROM entries WHERE id='D|file'`).Scan(&childPath); err != nil {
		t.Fatal(err)
	}
	if childPath != `D:\root\new\a.txt` {
		t.Fatalf("descendant path not cascaded: got %q", childPath)
	}
}

func utf16LE(s string) []byte {
	// Keep test helper local and simple (BMP-only is enough for this test).
	out := make([]byte, 0, len(s)*2)
	for _, r := range s {
		out = append(out, byte(r), byte(r>>8))
	}
	return out
}
