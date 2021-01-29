package net

import (
	"fmt"
	"strconv"
	"strings"
)

func ParseAddress(addr string) (ip string, port int, err error) {
	strs := strings.Split(addr, ":")
	if len(strs) != 2 {
		err = fmt.Errorf("parse address failed : %s", addr)
		return
	}
	ip = strings.TrimSpace(strs[0])
	port, err = strconv.Atoi(strings.TrimSpace(strs[1]))
	return
}
