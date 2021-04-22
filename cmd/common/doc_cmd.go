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
package common

import (
	"fmt"

	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

// genDocCommand is used to implement 'doc' command.
type genDocCommand struct {
	cmd  *cobra.Command
	path string
}

func newGenDocCommand(name string) *cobra.Command {
	genDocCommand := new(genDocCommand)

	genDocCommand.cmd = &cobra.Command{
		Use:               "doc",
		Short:             fmt.Sprintf("generate markdown documents for cmd:%s", name),
		Long:              fmt.Sprintf("generate markdown documents for cmd:%s", name),
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		SilenceUsage:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return genDocCommand.runGenDoc()
		},
	}

	genDocCommand.bindFlags()

	return genDocCommand.cmd
}

// bindFlags binds flags for specific command.
func (g *genDocCommand) bindFlags() {
	flagSet := g.cmd.Flags()

	flagSet.StringVar(&g.path, "path", "./", "destination dir of generated markdown documents")
}

func (g *genDocCommand) runGenDoc() error {
	_ = fileutils.MkdirAll(g.path)
	if !fileutils.IsDir(g.path) {
		return errors.Errorf("path %s is not dir, please check it", g.path)
	}

	return doc.GenMarkdownTree(g.cmd.Parent(), g.path)
}
