// Package timezone is the time zone the source tables are written in.
package timezone

import "time"

// Beijing is Beijing time, and what a time without a timezone means: UTC+8
// throughout (Asia/Shanghai's daylight saving of 1986-1991 is not
// upstream's).
var Beijing = time.FixedZone("UTC+8", 8*60*60)
