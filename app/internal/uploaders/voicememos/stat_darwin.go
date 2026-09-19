package voicememos

import "syscall"

func birthtime(stat *syscall.Stat_t) (int64, int64) {
	return stat.Birthtimespec.Sec, stat.Birthtimespec.Nsec
}
