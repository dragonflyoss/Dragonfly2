package tasks

import (
	"errors"

	"d7y.io/dragonfly/v2/manager/types"
)

const (
	PreheatImageType = "image"
	PreheatFileType  = "image"
)

func (t *task) CreatePreheat(json types.CreatePreheatRequest) (*types.Preheat, error) {
	if json.Type == PreheatImageType {
	}

	if json.Type == PreheatFileType {
	}

	return nil, errors.New("unknown type")
}

func (t *task) GetPreheat(id string) (*types.Preheat, error) {
	task, err := t.Server.GetBackend().GetState(id)
	if err != nil {
		return nil, err
	}

	return &types.Preheat{
		ID:       task.TaskUUID,
		Status:   task.State,
		CreateAt: task.CreatedAt,
	}, nil
}

// TODO preheats
// func (t *task) preheats(hostname string, files []PreHeatFile) error {
// signatures := []*machinerytasks.Signature{}
// for _, v := range files {
// args, err := json.Marshal(v)
// if err != nil {
// return err
// }

// signatures = append(signatures, &machinerytasks.Signature{
// Name:       internaltasks.PreheatTask,
// RoutingKey: internaltasks.GetSchedulerQueue(hostname).String(),
// Args: []machinerytasks.Arg{
// {
// Type:  "string",
// Value: string(args),
// },
// },
// })
// }

// group, _ := machinerytasks.NewGroup(signatures...)
// _, err := t.Server.SendGroup(group, 0)
// return err
// }
