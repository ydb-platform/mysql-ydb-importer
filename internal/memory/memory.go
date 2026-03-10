// Package memory provides available system RAM for sizing in-memory chunks.
package memory

import (
	"bufio"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// FreeBytes returns an estimate of available memory in bytes (free + reclaimable where available).
// Used to cap chunk size so large tables don't exhaust RAM.
func FreeBytes() uint64 {
	switch runtime.GOOS {
	case "linux":
		return freeLinux()
	case "darwin":
		return freeDarwin()
	default:
		return 0
	}
}

func freeLinux() uint64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()
	var memAvailable uint64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "MemAvailable:") {
			memAvailable = parseMeminfoKb(line)
			break
		}
	}
	if memAvailable == 0 {
		_, _ = f.Seek(0, 0)
		sc = bufio.NewScanner(f)
		var free, buffers, cached uint64
		for sc.Scan() {
			l := sc.Text()
			if strings.HasPrefix(l, "MemFree:") {
				free = parseMeminfoKb(l)
			} else if strings.HasPrefix(l, "Buffers:") {
				buffers = parseMeminfoKb(l)
			} else if strings.HasPrefix(l, "Cached:") {
				cached = parseMeminfoKb(l)
			}
		}
		memAvailable = free + buffers + cached
	}
	return memAvailable * 1024
}

func parseMeminfoKb(line string) uint64 {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	n, _ := strconv.ParseUint(fields[1], 10, 64)
	return n
}

func freeDarwin() uint64 {
	cmd := exec.Command("vm_stat")
	out, err := cmd.Output()
	if err != nil {
		return 0
	}
	const pageSize = 4096
	var free, inactive uint64
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "Pages free:") {
			free = parseVmStatNum(line)
		} else if strings.HasPrefix(line, "Pages inactive:") {
			inactive = parseVmStatNum(line)
		}
	}
	return (free + inactive) * pageSize
}

func parseVmStatNum(line string) uint64 {
	colon := strings.Index(line, ":")
	if colon < 0 {
		return 0
	}
	rest := strings.TrimSpace(line[colon+1:])
	rest = strings.TrimSuffix(rest, ".")
	n, _ := strconv.ParseUint(rest, 10, 64)
	return n
}
