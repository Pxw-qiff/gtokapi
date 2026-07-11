package platform

import "time"

// timeNow is a small indirection so callers can be overridden in tests.
var timeNow = func() time.Time { return time.Now() }

// NowSec returns the current time in seconds since epoch.
func NowSec() int64 { return timeNow().Unix() }
