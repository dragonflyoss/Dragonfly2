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
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"d7y.io/dragonfly/v2/internal/dfplugin"
	"d7y.io/dragonfly/v2/pkg/dfpath"
	"d7y.io/dragonfly/v2/pkg/source"
)

var PluginCmd = &cobra.Command{
	Use:               "plugin",
	Short:             "show plugin",
	Long:              `show the plugin details of dragonfly.`,
	Args:              cobra.NoArgs,
	DisableAutoGenTag: true,
	SilenceUsage:      true,
	Run: func(cmd *cobra.Command, args []string) {
		ListAvailableInTreePlugins()
		ListAvailableOutOfTreePlugins()
	},
}

func ListAvailableInTreePlugins() {
	clients := source.ListClients()
	for _, scheme := range clients {
		fmt.Printf("source plugin: %s, location: in-tree\n", scheme)
	}
}

func ListAvailableOutOfTreePlugins() {
	d, err := dfpath.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get plugin path: %q\n", err)
		return
	}

	fmt.Fprintf(os.Stderr, "search plugin in %s\n", d.PluginDir())
	files, err := os.ReadDir(d.PluginDir())
	if os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "no plugin found\n")
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "read plugin dir %s error: %s\n", d.PluginDir(), err)
		return
	}
	if len(files) == 0 {
		fmt.Fprintf(os.Stderr, "no out of tree plugin found\n")
		return
	}
	for _, file := range files {
		var attr []byte
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
			_, data, err := dfplugin.Load(d.PluginDir(), dfplugin.PluginType(typ), name, map[string]string{})
			if err != nil {
				fmt.Fprintf(os.Stderr, "not valid plugin binary format %s: %q\n", fileName, err)
				continue
			}
			attr, err = json.Marshal(data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "marshal attribute for %s error: %q\n", fileName, err)
				continue
			}
		default:
			fmt.Fprintf(os.Stderr, "not support plugin type: %s\n", typ)
			continue
		}

		fmt.Printf("%s plugin: %s, location: %s, attribute: %s\n", typ, name, fileName, string(attr))
	}
}
