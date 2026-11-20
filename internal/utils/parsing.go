package utils

func SplitLine(line string, delim rune) []string {
	output := make([]string, 0, 3)
	beginning := 0
	quoting := false
	escaping := false
	for index, symbol := range line {
		if symbol == '\\' {
			escaping = !escaping
			continue
		}

		if symbol == delim {
			if !escaping && !quoting {
				item := line[beginning:index]
				output = append(output, string(item))
				beginning = index + 1
			}
		} else if symbol == '"' {
			if !escaping {
				quoting = !quoting
			}
		}

		escaping = false
	}

	if beginning < len(line) {
		item := line[beginning:]
		output = append(output, string(item))
	}

	return output
}
