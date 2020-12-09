package types

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/base"
)

type Task struct {
	Url string `json:"url,omitempty"`
	// regex format, used for task id generator, assimilating different urls
	Filter string `json:"filter,omitempty"`
	// biz_id and md5 are used for task id generator to distinguish the same urls
	// md5 is also used to check consistency about file content
	BizId   string        `json:"biz_id,omitempty"`   // caller's biz id that can be any string
	UrlMata *base.UrlMeta `json:"url_mata,omitempty"` // downloaded file content md5

	PieceList []*Piece // Piece 列表
}
