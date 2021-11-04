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

	"github.com/spf13/cobra"

	"d7y.io/dragonfly/v2/version"
)

var VersionCmd = &cobra.Command{
	Use:               "version",
	Short:             "show version",
	Long:              `show the version details of dragonfly.`,
	Args:              cobra.NoArgs,
	DisableAutoGenTag: true,
	SilenceUsage:      true,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("MajorVersion:\t%s\n", version.Major)
		fmt.Printf("MinorVersion:\t%s\n", version.Minor)
		fmt.Printf("GitVersion:\t%s\n", version.GitVersion)
		fmt.Printf("GitCommit:\t%s\n", version.GitCommit)
		fmt.Printf("Platform:\t%s\n", version.Platform)
		fmt.Printf("BuildTime:\t%s\n", version.BuildTime)
		fmt.Printf("GoVersion:\t%s\n", version.GoVersion)
		fmt.Printf("Gotags:   \t%s\n", version.Gotags)
		fmt.Printf("Gogcflags:\t%s\n", version.Gogcflags)
	},
}
