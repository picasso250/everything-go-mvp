package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"sort"

	"golang.org/x/sys/windows"
)

const (
	onceFsctlEnumUSNData     = 0x000900b3
	onceFsctlQueryUSNJournal = 0x000900f4
)

func main() {
	vol := flag.String("volume", "D", "volume letter, e.g. D")
	windowUSN := flag.Int64("window-usn", 8*1024*1024, "scan window from NextUSN backwards")
	outBufBytes := flag.Uint("out-buf-bytes", 1024*1024, "DeviceIoControl output buffer")
	maxBatches := flag.Int("max-batches", 8, "max enum batches")
	flag.Parse()

	v, err := normalizeVolume(*vol)
	if err != nil {
		panic(err)
	}
	if v == 'C' {
		panic("refusing to read C: in probe_usn_once")
	}

	h, err := openVolumeReadonlyOnce(v)
	if err != nil {
		panic(err)
	}
	defer windows.CloseHandle(h)

	jdRaw, err := deviceIoControlOnce(h, onceFsctlQueryUSNJournal, nil, 64)
	if err != nil {
		panic(err)
	}
	if len(jdRaw) < 24 {
		panic("query journal output too small")
	}
	nextUSN := int64(binary.LittleEndian.Uint64(jdRaw[16:24]))

	lowUSN := nextUSN - *windowUSN
	if lowUSN < 0 {
		lowUSN = 0
	}

	counts := map[uint16]int{}
	total := 0
	startFRN := uint64(0)
	for i := 0; i < *maxBatches; i++ {
		in := make([]byte, 28)
		binary.LittleEndian.PutUint64(in[0:8], startFRN)
		binary.LittleEndian.PutUint64(in[8:16], uint64(lowUSN))
		binary.LittleEndian.PutUint64(in[16:24], uint64(nextUSN))
		binary.LittleEndian.PutUint16(in[24:26], 2)
		binary.LittleEndian.PutUint16(in[26:28], 4)

		out, err := deviceIoControlOnce(h, onceFsctlEnumUSNData, in, uint32(*outBufBytes))
		if err != nil {
			var errno windows.Errno
			if errors.As(err, &errno) {
				panic(fmt.Sprintf("enum usn failed (errno=%d)", uint32(errno)))
			}
			panic(err)
		}
		if len(out) < 8 {
			break
		}

		nextFRN := binary.LittleEndian.Uint64(out[0:8])
		pos := 8
		consumed := false
		for pos+8 <= len(out) {
			recLen := int(binary.LittleEndian.Uint32(out[pos : pos+4]))
			if recLen < 8 || pos+recLen > len(out) {
				break
			}
			major := binary.LittleEndian.Uint16(out[pos+4 : pos+6])
			counts[major]++
			total++
			pos += recLen
			consumed = true
		}
		if !consumed || nextFRN == startFRN {
			break
		}
		startFRN = nextFRN
	}

	if total == 0 {
		fmt.Printf("volume=%c total_records=0 versions=none next_usn=%d\n", v, nextUSN)
		return
	}

	keys := make([]int, 0, len(counts))
	for k := range counts {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)

	fmt.Printf("volume=%c total_records=%d next_usn=%d versions=", v, total, nextUSN)
	for i, k := range keys {
		if i > 0 {
			fmt.Print(",")
		}
		fmt.Printf("v%d:%d", k, counts[uint16(k)])
	}
	fmt.Println()
}

func normalizeVolume(raw string) (rune, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("invalid volume")
	}
	c := raw[0]
	if c >= 'a' && c <= 'z' {
		c = c - ('a' - 'A')
	}
	if c < 'A' || c > 'Z' {
		return 0, fmt.Errorf("invalid volume: %q", raw)
	}
	return rune(c), nil
}

func openVolumeReadonlyOnce(volume rune) (windows.Handle, error) {
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

func deviceIoControlOnce(h windows.Handle, code uint32, in []byte, outSize uint32) ([]byte, error) {
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
		return nil, err
	}
	return out[:returned], nil
}
