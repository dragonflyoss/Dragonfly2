package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"d7y.io/dragonfly/v2/manager/types"
)

type testCase struct {
	args types.PreheatArgs
	desc string
}

func TestGetManifest(t *testing.T) {
	p := &preheat{}

	testCases := []testCase{
		{
			args: types.PreheatArgs{
				URL:  "https://registry-1.docker.io/v2/dragonflyoss/busybox/manifests/1.35.0",
				Type: "image",
			},
			desc: "get opensource image layers",
		},
		{
			args: types.PreheatArgs{
				URL:      "https://registry-1.docker.io/v2/library/nginx/manifests/latest",
				Platform: "linux/amd64",
			},
			desc: "get docker official multi arch image layers",
		},
		{
			args: types.PreheatArgs{
				URL:      "xxx",
				Username: "xxx",
				Password: "xxx",
			},
			desc: "get harbor image layers",
		},
		{
			args: types.PreheatArgs{
				URL:      "xxx",
				Tag:      "xxx",
				Username: "xxx",
				Password: "xxx",
			},
			desc: "get ali image layers",
		},
		{
			args: types.PreheatArgs{
				URL:      "xxx",
				Username: "xxx",
				Password: "xxx",
			},
			desc: "get huawei image layers",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Log(tc.desc)
			layers, err := p.getImageLayers(context.Background(), tc.args)
			require.NoError(t, err)
			require.NotEmpty(t, layers)
		})
	}
}
