//go:build linux || darwin || freebsd || netbsd || openbsd

package main

import "syscall"

// readCPUTime returns total user and system CPU consumed by this process,
// in nanoseconds. Used to compute average CPU utilisation over the run.
//
// Children processes (e.g. the wasmer2 runtime spawning threads via cgo)
// are accounted for too, because the wall-clock work happens inside this
// process.
func readCPUTime() (userNs, sysNs uint64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	userNs = uint64(ru.Utime.Sec)*1e9 + uint64(ru.Utime.Usec)*1e3
	sysNs = uint64(ru.Stime.Sec)*1e9 + uint64(ru.Stime.Usec)*1e3
	return
}
