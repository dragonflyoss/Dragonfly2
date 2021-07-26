package tasks

import (
	"errors"

	"d7y.io/dragonfly/v2/manager/types"
)

const (
	PreheatImageType = "image"
	PreheatFileType  = "file"
)

func (t *task) CreatePreheat(json types.CreatePreheatRequest) (*types.Preheat, error) {
	if json.Type == PreheatImageType {
	}
	if json.Type == PreheatFileType {
	}

	return nil, errors.New("unknown type")
}

func (t *task) GetPreheat(id string) (*types.Preheat, error) {
	groupTaskState, err := t.GetGroupTaskState(id)
	if err != nil {
		return nil, err
	}

	return &types.Preheat{
		ID:        groupTaskState.GroupUUID,
		Status:    groupTaskState.State,
		CreatedAt: groupTaskState.CreatedAt,
	}, nil
}

// TODO preheats
// func (t *task) preheats(hostname string, files []PreHeatFile) error {
// signatures := []*machinerytasks.Signature{}
// for _, v := range files {
// if err != nil {
// return err
// }

// signatures = append(signatures, &machinerytasks.Signature{
// Name:       internaltasks.PreheatTask,
// RoutingKey: internaltasks.GetSchedulerQueue(hostname).String(),
// Args: internaltasks.Marshal(),
// })
// }

// group, _ := machinerytasks.NewGroup(signatures...)
// _, err := t.Server.SendGroup(group, 0)
// return err
// }
