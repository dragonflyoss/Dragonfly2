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

func TestRoleName(t *testing.T) {
	tests := []struct {
		object           string
		action           string
		exceptedRoleName string
	}{
		{
			object:           "users",
			action:           "read",
			exceptedRoleName: "users:read",
		},
		{
			object:           "cdns",
			action:           "write",
			exceptedRoleName: "cdns:*",
		},
	}

	for _, tt := range tests {
		roleName := RoleName(tt.object, tt.action)
		if roleName != tt.exceptedRoleName {
			t.Errorf("RoleName(%v, %v) = %v, want %v", tt.object, tt.action, roleName, tt.exceptedRoleName)
		}
	}

}
func TestHttpMethodToAction(t *testing.T) {
	tests := []struct {
		method         string
		exceptedAction string
	}{
		{
			method:         "GET",
			exceptedAction: "read",
		},
		{
			method:         "POST",
			exceptedAction: "*",
		},
		{
			method:         "UNKNOWN",
			exceptedAction: "",
		},
	}

	for _, tt := range tests {
		action := HTTPMethodToAction(tt.method)
		if action != tt.exceptedAction {
			t.Errorf("HttpMethodToAction(%v) = %v, want %v", tt.method, action, tt.exceptedAction)
		}
	}

}
