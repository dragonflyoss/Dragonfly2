package tasks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTaskGetSchedulerQueue(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		expect   func(t *testing.T, result Queue, err error)
	}{
		{
			name:     "get scheduler queue succeeded",
			hostname: "foo",
			expect: func(t *testing.T, result Queue, err error) {
				assert := assert.New(t)
				assert.Equal(Queue("scheduler_foo"), result)
			},
		},
		{
			name:     "get scheduler queue with empty hostname",
			hostname: "",
			expect: func(t *testing.T, result Queue, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "empty hostname config is not specified")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			queue, err := GetSchedulerQueue(tc.hostname)
			tc.expect(t, queue, err)
		})
	}
}

func TestTaskGetCDNQueue(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		expect   func(t *testing.T, result Queue, err error)
	}{
		{
			name:     "get cdn queue succeeded",
			hostname: "foo",
			expect: func(t *testing.T, result Queue, err error) {
				assert := assert.New(t)
				assert.Equal(Queue("cdn_foo"), result)
			},
		},
		{
			name:     "get cdn queue with empty hostname",
			hostname: "",
			expect: func(t *testing.T, result Queue, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "empty hostname config is not specified")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			queue, err := GetCDNQueue(tc.hostname)
			tc.expect(t, queue, err)
		})
	}
}
