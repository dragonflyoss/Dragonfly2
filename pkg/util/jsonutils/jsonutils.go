package jsonutils

import "encoding/json"

func MarshalMap(m map[string]interface{}) (map[string][]byte, error) {
	mb := make(map[string][]byte)
	for k, v := range m {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		mb[k] = b
	}

	return mb, nil
}
