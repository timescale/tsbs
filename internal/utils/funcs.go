package utils

import (
	"fmt"
	"time"
)

func IsIn(s string, arr []string) bool {
	for _, x := range arr {
		if s == x {
			return true
		}
	}
	return false
}

// ParseUTCTime parses a string-represented time of the format 2006-01-02T15:04:05Z07:00
func ParseUTCTime(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

const (
	errInvalidGroupsFmt = "incorrect interleaved groups configuration: id %d >= total groups %d"
	errTotalGroupsZero  = "incorrect interleaved groups configuration: total groups = 0"
)

// ValidateGroups checks validity of combination groupID and totalGroups
func ValidateGroups(groupID, totalGroupsNum uint) error {
	if totalGroupsNum == 0 {
		// Need at least one group
		return fmt.Errorf(errTotalGroupsZero)
	}
	if groupID >= totalGroupsNum {
		// Need reasonable groupID
		return fmt.Errorf(errInvalidGroupsFmt, groupID, totalGroupsNum)
	}
	return nil
}
