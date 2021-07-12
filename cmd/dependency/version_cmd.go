/*
 *     Copyright 2020 The Dragonfly Authors
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

package dependency

import (
	"fmt"

	"d7y.io/dragonfly.v2/version"
	"github.com/spf13/cobra"
)

var VersionCmd = &cobra.Command{
	Use:               "version",
	Short:             "show version",
	Long:              `show the version details of dragonfly.`,
	Args:              cobra.NoArgs,
	DisableAutoGenTag: true,
	SilenceUsage:      true,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Major:%s Minor:%s\n", version.Major, version.Minor)
		fmt.Printf("GitVersion:%s\n", version.GitVersion)
		fmt.Printf("Platform:%s GoVersion:%s BuildDay:%s\n", version.Platform, version.GoVersion, version.BuildDay)
	},
}
