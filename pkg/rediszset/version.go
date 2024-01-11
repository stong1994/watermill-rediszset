package rediszset

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

func getVersion(info string) ([3]int, error) {
	re := regexp.MustCompile(`redis_version:(\d+\.\d+\.\d+)`)
	match := re.FindStringSubmatch(info)
	if len(match) > 1 {
		vs := strings.Split(match[1], ".")
		if len(vs) == 3 {
			var version [3]int
			for i, v := range vs {
				num, err := strconv.Atoi(v)
				if err != nil {
					return [3]int{}, fmt.Errorf("invalid redis info: %s", info)
				}
				version[i] = num
			}
			return version, nil
		}
	}
	return [3]int{}, fmt.Errorf("invalid redis info: %s", info)
}
