package config

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

var DefaultSupernodesValue = &SupernodesValue{
	Nodes: []string{
		fmt.Sprintf("%s:%d", DefaultSupernodeIP, DefaultSupernodePort),
	},
}

type SupernodesValue struct {
	Nodes []string
}

// String implements the pflag.Value interface.
func (sv *SupernodesValue) String() string {
	var result []string
	for _, v := range sv.Nodes {
		result = append(result, v)
	}
	return strings.Join(result, ",")
}

// Set implements the pflag.Value interface.
func (sv *SupernodesValue) Set(value string) error {
	nodes := strings.Split(value, ",")
	for _, n := range nodes {
		v := strings.Split(n, "=")
		if len(v) == 0 || len(v) > 2 {
			return errors.New("invalid nodes")
		}
		// ignore weight
		node := v[0]
		vv := strings.Split(node, ":")
		if len(vv) >= 2 {
			return errors.New("invalid nodes")
		}
		if len(vv) == 1 {
			node = fmt.Sprintf("%s:%d", node, DefaultSupernodePort)
		}
		sv.Nodes = append(sv.Nodes, node)
	}
	return nil
}

// Type implements the pflag.Value interface.
func (sv *SupernodesValue) Type() string {
	return "supernodes"
}
