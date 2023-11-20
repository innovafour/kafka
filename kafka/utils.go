package kafka

import (
	"strings"
)

func GetKeyFromHeaders(headers map[string]string, key string) string {
	var result string

	resultHeader := getValueIgnoringCase(headers, key)
	if resultHeader != "" {
		result = resultHeader
	}

	return result
}

func getValueIgnoringCase(headers map[string]string, key string) string {
	keyLower := strings.ToLower(key)

	for k, v := range headers {
		if strings.ToLower(k) == keyLower {
			return v
		}
	}

	return ""
}
