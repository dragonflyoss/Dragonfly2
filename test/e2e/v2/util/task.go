/*
 *     Copyright 2024 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package util

import (
	"errors"
	"fmt"
	"strings"
)

const (
	clientContentDir = "/var/lib/dragonfly/content"
)

type TaskMetadata struct {
	ID     string
	Sha256 string
}

// Check files is exist or not.
func CheckFilesExist(pods []*PodExec, taskID string) bool {
	for _, pod := range pods {
		contentPath := fmt.Sprintf("%s/tasks/%s/%s", clientContentDir, taskID[:3], taskID)
		if _, err := pod.Command("ls", contentPath).CombinedOutput(); err != nil {
			// If the path does not exist, skip this client.
			fmt.Printf("path %s does not exist: %s\n", contentPath, err.Error())
			continue
		}
		return true
	}
	return false
}

func CalculateSha256ByTaskID(pods []*PodExec, taskID string) (string, error) {
	var sha256sum string
	for _, pod := range pods {
		contentPath := fmt.Sprintf("%s/tasks/%s/%s", clientContentDir, taskID[:3], taskID)
		if _, err := pod.Command("ls", contentPath).CombinedOutput(); err != nil {
			// If the path does not exist, skip this client.
			fmt.Printf("path %s does not exist: %s\n", contentPath, err.Error())
			continue
		}

		// Calculate sha256sum of the task content.
		out, err := pod.Command("sh", "-c", fmt.Sprintf("sha256sum %s", contentPath)).CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("calculate sha256sum of %s failed: %s", contentPath, err.Error())
		}

		fmt.Println("sha256sum: " + string(out))
		sha256sum = strings.Split(string(out), " ")[0]
		break
	}

	if sha256sum == "" {
		return "", errors.New("can not found sha256sum")
	}

	return sha256sum, nil
}

func CalculateSha256ByOutput(pods []*PodExec, output string) (string, error) {
	var sha256sum string
	for _, pod := range pods {
		if _, err := pod.Command("ls", output).CombinedOutput(); err != nil {
			// If the path does not exist, skip this client.
			fmt.Printf("path %s does not exist: %s\n", output, err.Error())
			continue
		}

		// Calculate sha256sum of the output content.
		out, err := pod.Command("sh", "-c", fmt.Sprintf("sha256sum %s", output)).CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("calculate sha256sum of %s failed: %s", output, err.Error())
		}

		fmt.Println("sha256sum: " + string(out))
		sha256sum = strings.Split(string(out), " ")[0]
		break
	}

	if sha256sum == "" {
		return "", errors.New("can not found sha256sum")
	}

	return sha256sum, nil
}
