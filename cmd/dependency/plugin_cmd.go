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
	"io/ioutil"
	"os"

	"github.com/spf13/cobra"

	"d7y.io/dragonfly/v2/internal/dfpath"
	"d7y.io/dragonfly/v2/internal/dfplugin"
)

var PluginCmd = &cobra.Command{
	Use:               "plugin",
	Short:             "show plugin",
	Long:              `show the plugin details of dragonfly.`,
	Args:              cobra.NoArgs,
	DisableAutoGenTag: true,
	SilenceUsage:      true,
	Run: func(cmd *cobra.Command, args []string) {
		ListAvailablePlugins()
	},
}

func ListAvailablePlugins() {
	fmt.Fprintf(os.Stderr, "search plugin in %s\n", dfpath.PluginsDir)
	files, err := ioutil.ReadDir(dfpath.PluginsDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read plugin dir %s error: %s\n", dfpath.PluginsDir, err)
		return
	}
	for _, file := range files {
		fileName := file.Name()
		if file.IsDir() {
			fmt.Fprintf(os.Stderr, "not support directory: %s\n", fileName)
			continue
		}
		subs := dfplugin.PluginFormatExpr.FindStringSubmatch(fileName)
		if len(subs) != 3 {
			fmt.Fprintf(os.Stderr, "not valid plugin name: %s\n", fileName)
			continue
		}
		typ, name := subs[1], subs[2]
		switch typ {
		case string(dfplugin.PluginTypeResource), string(dfplugin.PluginTypeScheduler), string(dfplugin.PluginTypeManager):
			_, _, err = dfplugin.Load(dfplugin.PluginType(typ), name, map[string]string{})
			if err != nil {
				fmt.Fprintf(os.Stderr, "not valid plugin binary format %s: %q\n", fileName, err)
				continue
			}
		default:
			fmt.Fprintf(os.Stderr, "not support plugin type: %s\n", typ)
			continue
		}

		fmt.Printf("%s: %s %s\n", fileName, typ, name)
	}
}
