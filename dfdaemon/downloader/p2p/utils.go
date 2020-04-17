package p2p

import (
	"fmt"
	"strings"
)

func FlattenHeader(header map[string][]string) []string {
	var res []string
	for key, value := range header {
		// discard HTTP host header for backing to source successfully
		if strings.EqualFold(key, "host") {
			continue
		}
		if len(value) > 0 {
			for _, v := range value {
				res = append(res, fmt.Sprintf("%s:%s", key, v))
			}
		} else {
			res = append(res, fmt.Sprintf("%s:%s", key, ""))
		}
	}
	return res
}
