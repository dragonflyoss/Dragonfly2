package configsvc

import (
	"d7y.io/dragonfly/v2/pkg/idgen"
	"fmt"
)

func NewConfigID() string {
	return fmt.Sprintf("%s", idgen.UUIDString())
}
