package slidingwindow

import (
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

type testcase struct {
	name  string
	len   uint
	size  uint
	count uint
	test  func(test *testcase) error
}

func TestWindow(t *testing.T) {
	assert := testifyassert.New(t)
	tests := []testcase{
		{
			name:  "Add_Second_Count",
			len:   10,
			size:  8,
			count: 1,
			test: func(testcase *testcase) error {
				w, err := NewWindow(testcase.len, testcase.size)
				if err != nil {
					return err
				}

				w.AddCount(testcase.count)
				assert.Equal(w.Start(), uint(0))
				assert.Equal(w.Size(), testcase.size)
				assert.Equal(w.Len(), testcase.len)
				assert.Equal(w.IsFinished(), false)
				assert.Equal(w.Status().size, testcase.size)
				assert.Equal(w.Status().start, uint(0))

				return nil
			},
		},
		{
			name:  "Add_First_Count",
			len:   2,
			size:  1,
			count: 0,
			test: func(testcase *testcase) error {
				w, err := NewWindow(testcase.len, testcase.size)
				if err != nil {
					return err
				}

				w.AddCount(testcase.count)
				assert.Equal(w.Start(), uint(1))
				assert.Equal(w.Size(), testcase.size)
				assert.Equal(w.Len(), testcase.len)
				assert.Equal(w.IsFinished(), false)
				assert.Equal(w.Status().size, testcase.size)
				assert.Equal(w.Status().start, uint(1))

				w.AddCount(1)
				assert.Equal(w.Start(), uint(2))
				assert.Equal(w.Size(), testcase.size)
				assert.Equal(w.Len(), testcase.len)
				assert.Equal(w.IsFinished(), true)
				assert.Equal(w.Status().size, testcase.size)
				assert.Equal(w.Status().start, uint(2))

				return nil
			},
		},
		{
			name:  "Invalid_Size",
			len:   10,
			size:  11,
			count: 0,
			test: func(testcase *testcase) error {
				_, err := NewWindow(testcase.len, testcase.size)
				assert.EqualError(err, "Len and size values are illegal")

				return nil
			},
		},
		{
			name:  "Invalid_Len_And_Size",
			len:   10,
			size:  10,
			count: 0,
			test: func(testcase *testcase) error {
				_, err := NewWindow(testcase.len, testcase.size)
				assert.EqualError(err, "Len and size values are illegal")

				return nil
			},
		},
		{
			name:  "Invalid_Len_And_Size",
			len:   0,
			size:  0,
			count: 0,
			test: func(testcase *testcase) error {
				_, err := NewWindow(testcase.len, testcase.size)
				assert.EqualError(err, "Len and size values are illegal")

				return nil
			},
		},
	}

	for _, testcase := range tests {
		err := testcase.test(&testcase)
		if err != nil {
			t.Errorf("%s failed: err=%q", testcase.name, err)
		}
	}
}
