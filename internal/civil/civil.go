// Package civil turns a civil date in an IANA time zone into the half-open
// UTC interval of its instants, and validates the time_zone query value.
package civil

import (
	"errors"
	"regexp"
	"time"
	_ "time/tzdata" // the image ships its own zone data
)

// ErrInvalidZone is returned for an unsupported time_zone value.
var ErrInvalidZone = errors.New("civil: unsupported time zone")

// ErrInvalidDate is returned for a date that is not a strict YYYY-MM-DD.
var ErrInvalidDate = errors.New("civil: invalid date")

var zonePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_+-]*(/[A-Za-z0-9_+-]+)*$`)

// Zone validates the time_zone values of a request: absent means UTC; an
// empty, repeated, numeric, path-like or Local value is rejected.
func Zone(values []string, present bool) (*time.Location, string, error) {
	if !present {
		return time.UTC, "UTC", nil
	}
	if len(values) != 1 || values[0] == "" || values[0] == "Local" || !zonePattern.MatchString(values[0]) {
		return nil, "", ErrInvalidZone
	}
	loc, err := time.LoadLocation(values[0])
	if err != nil {
		return nil, "", ErrInvalidZone
	}
	return loc, values[0], nil
}

// DateBounds returns [start, end) of the civil date key in location. exists
// is false for a skipped date. It intersects the date with each actual
// zone-offset interval, so nonexistent midnights are never normalized and
// both sides of a repeated hour are included.
func DateBounds(key string, location *time.Location) (time.Time, time.Time, bool, error) {
	civil, err := time.Parse(time.DateOnly, key)
	if err != nil || civil.Format(time.DateOnly) != key || location == nil {
		return time.Time{}, time.Time{}, false, ErrInvalidDate
	}
	next := civil.AddDate(0, 0, 1)
	windowStart, windowEnd := civil.Add(-48*time.Hour), next.Add(48*time.Hour)
	var first, last time.Time
	found := false
	for at := windowStart; at.Before(windowEnd); {
		local := at.In(location)
		_, offset := local.Zone()
		_, end := local.ZoneBounds()
		if end.IsZero() || end.After(windowEnd) {
			end = windowEnd
		}
		start := civil.Add(-time.Duration(offset) * time.Second)
		stop := next.Add(-time.Duration(offset) * time.Second)
		if start.Before(at) {
			start = at
		}
		if stop.After(end) {
			stop = end
		}
		if start.Before(stop) {
			if !found || start.Before(first) {
				first = start
			}
			if !found || stop.After(last) {
				last = stop
			}
			found = true
		}
		at = end
	}
	if !found {
		return time.Time{}, time.Time{}, false, nil
	}
	return first.UTC(), last.UTC(), true, nil
}
