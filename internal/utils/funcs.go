package utils

func IsIn(s string, arr []string) bool {
	for _, x := range arr {
		if s == x {
			return true
		}
	}
	return false
}

