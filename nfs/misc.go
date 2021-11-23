package nfs

import (
	"syscall"
	"time"
)

func nowTimespec() (ts syscall.Timespec) {
	t := time.Now()
	return syscall.Timespec{Sec: t.Unix(), Nsec: int64(t.Nanosecond())}
}
