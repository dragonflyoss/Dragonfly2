package rbac

import (
	"testing"
)

func TestGetApiGroupName(t *testing.T) {
	tests := []struct {
		path              string
		exceptedGroupName string
		hasError          bool
	}{
		{
			path:              "/api/v1/users",
			exceptedGroupName: "users",
			hasError:          false,
		},
		{
			path:              "/api/user",
			exceptedGroupName: "",
			hasError:          true,
		},
	}

	for _, tt := range tests {
		groupName, err := GetAPIGroupName(tt.path)
		if tt.hasError {
			if err == nil {
				t.Errorf("GetApiGroupName(%s) should return error", tt.path)
			}
		}

		if groupName != tt.exceptedGroupName {
			t.Errorf("GetApiGroupName(%v) = %v, want %v", tt.path, groupName, tt.exceptedGroupName)
		}
	}

}
