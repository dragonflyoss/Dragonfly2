package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MakeNamespaceKeyInManager(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		expect    func(t *testing.T, s string)
	}{
		{
			name:      "make namespace key in manager",
			namespace: "namespace",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:namespace")
			},
		},
		{
			name:      "namespace is empty",
			namespace: "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeNamespaceKeyInManager(tc.namespace))
		})
	}
}

func Test_MakeKeyInManager(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		id        string
		expect    func(t *testing.T, s string)
	}{
		{
			name:      "make key in manager",
			namespace: "namespace",
			id:        "id",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:namespace:id")
			},
		},
		{
			name:      "namespace is empty",
			namespace: "",
			id:        "id",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager::id")
			},
		},
		{
			name:      "key is empty",
			namespace: "namespace",
			id:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:namespace:")
			},
		},
		{
			name:      "namespace and key are empty",
			namespace: "",
			id:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager::")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeKeyInManager(tc.namespace, tc.id))
		})
	}
}

func Test_MakeSeedPeerKeyInManager(t *testing.T) {
	tests := []struct {
		name      string
		clusterID uint
		hostname  string
		ip        string
		expect    func(t *testing.T, s string)
	}{
		{
			name:      "make seed peer key in manager",
			clusterID: 1,
			hostname:  "hostname",
			ip:        "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:seed-peers:1-hostname-127.0.0.1")
			},
		},
		{
			name:      "hostname is empty",
			clusterID: 1,
			hostname:  "",
			ip:        "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:seed-peers:1--127.0.0.1")
			},
		},
		{
			name:      "ip is empty",
			clusterID: 1,
			hostname:  "hostname",
			ip:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:seed-peers:1-hostname-")
			},
		},
		{
			name:      "hostname and ip are empty",
			clusterID: 1,
			hostname:  "",
			ip:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:seed-peers:1--")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeSeedPeerKeyInManager(tc.clusterID, tc.hostname, tc.ip))
		})
	}
}

func Test_MakeSchedulerKeyInManager(t *testing.T) {
	tests := []struct {
		name      string
		clusterID uint
		hostname  string
		ip        string
		expect    func(t *testing.T, s string)
	}{
		{
			name:      "make scheduler key in manager",
			clusterID: 1,
			hostname:  "hostname",
			ip:        "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:schedulers:1-hostname-127.0.0.1")
			},
		},
		{
			name:      "hostname is empty",
			clusterID: 1,
			hostname:  "",
			ip:        "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:schedulers:1--127.0.0.1")
			},
		},
		{
			name:      "ip is empty",
			clusterID: 1,
			hostname:  "hostname",
			ip:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:schedulers:1-hostname-")
			},
		},
		{
			name:      "hostname and ip are empty",
			clusterID: 1,
			hostname:  "",
			ip:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:schedulers:1--")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeSchedulerKeyInManager(tc.clusterID, tc.hostname, tc.ip))
		})
	}
}

func Test_MakePeerKeyInManager(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		ip       string
		expect   func(t *testing.T, s string)
	}{
		{
			name:     "make peer key in manager",
			hostname: "hostname",
			ip:       "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:hostname-127.0.0.1")
			},
		},
		{
			name:     "hostname is empty",
			hostname: "",
			ip:       "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:-127.0.0.1")
			},
		},
		{
			name:     "ip is empty",
			hostname: "hostname",
			ip:       "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:hostname-")
			},
		},
		{
			name:     "hostname and ip are empty",
			hostname: "",
			ip:       "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:-")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakePeerKeyInManager(tc.hostname, tc.ip))
		})
	}
}

func Test_MakeSchedulersKeyForPeerInManager(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		ip       string
		expect   func(t *testing.T, s string)
	}{
		{
			name:     "make scheduler key for peer in manager",
			hostname: "hostname",
			ip:       "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:hostname-127.0.0.1:schedulers")
			},
		},
		{
			name:     "hostname is empty",
			hostname: "",
			ip:       "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:-127.0.0.1:schedulers")
			},
		},
		{
			name:     "ip is empty",
			hostname: "hostname",
			ip:       "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:hostname-:schedulers")
			},
		},
		{
			name:     "hostname and ip are empty",
			hostname: "",
			ip:       "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:-:schedulers")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeSchedulersKeyForPeerInManager(tc.hostname, tc.ip))
		})
	}
}

func Test_MakeApplicationsKeyInManager(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s string)
	}{
		{
			name: "make applications key in manager",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:applications")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeApplicationsKeyInManager())
		})
	}
}

func Test_MakeBucketKeyInManager(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		expect    func(t *testing.T, s string)
	}{
		{
			name:      "make bucket key in manager",
			namespace: "namespace",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:buckets:namespace")
			},
		},
		{
			name:      "namespace is empty",
			namespace: "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:buckets:")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeBucketKeyInManager(tc.namespace))
		})
	}
}

func Test_MakeNamespaceKeyInScheduler(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		expect    func(t *testing.T, s string)
	}{
		{
			name:      "make namespace key in scheduler",
			namespace: "namespace",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:namespace")
			},
		},
		{
			name:      "namespace is empty",
			namespace: "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeNamespaceKeyInScheduler(tc.namespace))
		})
	}
}

func Test_MakeKeyInScheduler(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		id        string
		expect    func(t *testing.T, s string)
	}{
		{
			name:      "make key in scheduler",
			namespace: "namespace",
			id:        "id",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:namespace:id")
			},
		},
		{
			name:      "namespace is empty",
			namespace: "",
			id:        "id",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler::id")
			},
		},
		{
			name:      "id is empty",
			namespace: "namespace",
			id:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:namespace:")
			},
		},
		{
			name:      "namespace and id are empty",
			namespace: "",
			id:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler::")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeKeyInScheduler(tc.namespace, tc.id))
		})
	}
}

func Test_MakeNetworkTopologyKeyInScheduler(t *testing.T) {
	tests := []struct {
		name       string
		srcHostID  string
		destHostID string
		expect     func(t *testing.T, s string)
	}{
		{
			name:       "make network topology key in scheduler",
			srcHostID:  "src",
			destHostID: "dest",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:network-topology:src:dest")
			},
		},
		{
			name:       "source host id is empty",
			srcHostID:  "",
			destHostID: "dest",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:network-topology::dest")
			},
		},
		{
			name:       "destination host id is empty",
			srcHostID:  "src",
			destHostID: "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:network-topology:src:")
			},
		},
		{
			name:       "source host id and destination host id are empty",
			srcHostID:  "",
			destHostID: "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:network-topology::")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeNetworkTopologyKeyInScheduler(tc.srcHostID, tc.destHostID))
		})
	}
}

func Test_MakeProbesKeyInScheduler(t *testing.T) {
	tests := []struct {
		name       string
		srcHostID  string
		destHostID string
		expect     func(t *testing.T, s string)
	}{
		{
			name:       "make probes key in scheduler",
			srcHostID:  "src",
			destHostID: "dest",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:probes:src:dest")
			},
		},
		{
			name:       "source host id is empty",
			srcHostID:  "",
			destHostID: "dest",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:probes::dest")
			},
		},
		{
			name:       "destination host id is empty",
			srcHostID:  "src",
			destHostID: "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:probes:src:")
			},
		},
		{
			name:       "source host id and destination host id are empty",
			srcHostID:  "",
			destHostID: "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:probes::")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeProbesKeyInScheduler(tc.srcHostID, tc.destHostID))
		})
	}
}

func Test_MakeProbedCountKeyInScheduler(t *testing.T) {
	tests := []struct {
		name   string
		hostID string
		expect func(t *testing.T, s string)
	}{
		{
			name:   "make probed count key in scheduler",
			hostID: "host_id",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:probed-count:host_id")
			},
		},
		{
			name:   "host id is empty",
			hostID: "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:probed-count:")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeProbedCountKeyInScheduler(tc.hostID))
		})
	}
}

func Test_MakeProbedAtKeyInScheduler(t *testing.T) {
	tests := []struct {
		name   string
		hostID string
		expect func(t *testing.T, s string)
	}{
		{
			name:   "make probed at key in scheduler",
			hostID: "host_id",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal("scheduler:probed-at:host_id", s)
			},
		},
		{
			name:   "host id is empty",
			hostID: "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal("scheduler:probed-at:", s)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeProbedAtKeyInScheduler(tc.hostID))
		})
	}
}
