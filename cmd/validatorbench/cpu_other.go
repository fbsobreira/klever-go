//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd

package main

// readCPUTime is a stub for platforms without syscall.Getrusage. The
// CPU section of the report will simply read 0 — operators on those
// platforms get the rest of the metrics intact.
func readCPUTime() (userNs, sysNs uint64) { return 0, 0 }
