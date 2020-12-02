package plugins

import (
	"fmt"

	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"

	"github.com/sirupsen/logrus"
)

var mgr = NewManager()

// SetManager sets a Manager implementation instead of the default one.
func SetManager(m Manager) {
	mgr = m
}

// Initialize builds all plugins defined in config file.
func Initialize(cfg *config.Config) error {
	for pt, value := range cfg.Plugins {
		for _, v := range value {
			if !v.Enabled {
				logrus.Infof("plugin[%s][%s] is disabled", pt, v.Name)
				continue
			}
			builder := mgr.GetBuilder(pt, v.Name)
			if builder == nil {
				return fmt.Errorf("cannot find builder to create plugin[%s][%s]",
					pt, v.Name)
			}
			p, err := builder(v.Config)
			if err != nil {
				return fmt.Errorf("failed to build plugin[%s][%s]: %v",
					pt, v.Name, err)
			}
			mgr.AddPlugin(p)
			logrus.Infof("add plugin[%s][%s]", pt, v.Name)
		}
	}
	return nil
}

// RegisterPlugin register a plugin builder that will be called to create a new
// plugin instant when cdnNode starts.
func RegisterPlugin(pt config.PluginType, name string, builder Builder) {
	mgr.AddBuilder(pt, name, builder)
}

// GetPlugin returns a plugin instant with the giving plugin type and name.
func GetPlugin(pt config.PluginType, name string) Plugin {
	return mgr.GetPlugin(pt, name)
}
