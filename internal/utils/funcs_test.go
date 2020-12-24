package utils

import (
	"fmt"
	"testing"
)

func TestValidateGroups(t *testing.T) {
	cases := []struct {
		desc        string
		groupID     uint
		totalGroups uint
		errMsg      string
	}{
		{
			desc:        "id < total, no err",
			groupID:     0,
			totalGroups: 1,
		},
		{
			desc:        "id = total, should err",
			groupID:     1,
			totalGroups: 1,
			errMsg:      fmt.Sprintf(errInvalidGroupsFmt, 1, 1),
		},
		{
			desc:        "id > total, should err",
			groupID:     2,
			totalGroups: 1,
			errMsg:      fmt.Sprintf(errInvalidGroupsFmt, 2, 1),
		},
		{
			desc:        "total = 0, should err",
			groupID:     0,
			totalGroups: 0,
			errMsg:      errTotalGroupsZero,
		},
	}
	for _, c := range cases {
		err := ValidateGroups(c.groupID, c.totalGroups)
		if c.errMsg == "" && err != nil {
			t.Errorf("%s: unexpected error: %v", c.desc, err)
		} else if c.errMsg != "" && err == nil {
			t.Errorf("%s: unexpected lack of error", c.desc)
		} else if err != nil && err.Error() != c.errMsg {
			t.Errorf("%s: incorrect error: got %s want %s", c.desc, err.Error(), c.errMsg)
		}
	}
}
