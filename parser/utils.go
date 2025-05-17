package parser

import (
	"strings"
)

func toLowerExceptQuotes(s string) string {
	var result strings.Builder
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(s); i++ {
		ch := s[i]

		if ch == '\'' && !inDoubleQuote {
			inSingleQuote = !inSingleQuote
			result.WriteByte(ch)
			continue
		}

		if ch == '"' && !inSingleQuote {
			inDoubleQuote = !inDoubleQuote
			result.WriteByte(ch)
			continue
		}

		if inSingleQuote || inDoubleQuote {
			result.WriteByte(ch)
		} else {
			result.WriteByte(strings.ToLower(string(ch))[0])
		}
	}

	return result.String()
}
