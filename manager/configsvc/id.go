package configsvc

import (
	"fmt"

	"d7y.io/dragonfly/v2/internal/idgen"
)

func NewUUID(prefix string) string {
	return fmt.Sprintf("%s%s", prefix, idgen.UUIDString())
}
