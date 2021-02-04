package clientutil

import (
	"encoding/json"
	"time"

	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v3"
)

// RateLimit is a wrapper for rate.Limit, support json and yaml unmarshal function
// yaml example 1:
//   rate_limit: 2097152 # 2MiB
// yaml example 2:
//   rate_limit: 2MiB
type RateLimit struct {
	rate.Limit
}

func (r *RateLimit) UnmarshalJSON(b []byte) error {
	return r.unmarshal(json.Unmarshal, b)
}

func (r *RateLimit) UnmarshalYAML(node *yaml.Node) error {
	return r.unmarshal(yaml.Unmarshal, []byte(node.Value))
}

func (r *RateLimit) unmarshal(unmarshal func(in []byte, out interface{}) (err error), b []byte) error {
	var v interface{}
	if err := unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		r.Limit = rate.Limit(value)
		return nil
	case string:
		limit, err := units.RAMInBytes(value)
		if err != nil {
			return errors.WithMessage(err, "invalid port")
		}
		r.Limit = rate.Limit(limit)
		return nil
	default:
		return errors.New("invalid port")
	}
}

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	return d.unmarshal(v)
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var v interface{}
	switch node.Kind {
	case yaml.ScalarNode:
		switch node.Tag {
		case "!!int":
			var i int
			if err := node.Decode(&i); err != nil {
				return err
			}
			v = i
		case "!!str":
			var i string
			if err := node.Decode(&i); err != nil {
				return err
			}
			v = i
		default:
			return errors.New("invalid duration")
		}
	default:
		return errors.New("invalid duration")
	}
	return d.unmarshal(v)
}

func (d *Duration) unmarshal(v interface{}) error {
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case int:
		d.Duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid duration")
	}
}