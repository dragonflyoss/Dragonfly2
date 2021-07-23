package tasks

import (
	"encoding/json"

	internaltasks "d7y.io/dragonfly/v2/internal/tasks"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	machinerytasks "github.com/RichardKnop/machinery/v1/tasks"
)

type Task interface {
	preheats(string, []PreHeatFile) error
}

type task struct {
	*internaltasks.Tasks
}

func New(cfg *config.RedisConfig) (Task, error) {
	t, err := internaltasks.New(&internaltasks.Config{
		Host:      cfg.Host,
		Port:      cfg.Port,
		Password:  cfg.Password,
		BrokerDB:  cfg.BrokerDB,
		BackendDB: cfg.BackendDB,
	}, internaltasks.GlobalQueue)
	if err != nil {
		return nil, err
	}

	return &task{t}, nil
}

type PreHeatFile struct {
	URL     string
	URLMeta *base.UrlMeta
	Filter  string
}

func (t *task) preheats(hostname string, files []PreHeatFile) error {
	signatures := []*machinerytasks.Signature{}
	for _, v := range files {
		args, err := json.Marshal(v)
		if err != nil {
			return err
		}

		signatures = append(signatures, &machinerytasks.Signature{
			Name:       internaltasks.PreheatTask,
			RoutingKey: internaltasks.GetSchedulerQueue(hostname).String(),
			Args: []machinerytasks.Arg{
				{
					Type:  "string",
					Value: string(args),
				},
			},
		})
	}

	group, _ := machinerytasks.NewGroup(signatures...)
	_, err := t.Server.SendGroup(group, 0)
	return err
}
