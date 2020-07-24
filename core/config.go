package core

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

type HermesConfig struct {
	Port    int            `yaml:"port,omitempty"`
	Drivers []DriverConfig `yaml:"drivers,omitempty"`
}

type DriverConfig struct {
	Name          string   `yaml:"name,omitempty"`
	IsMainStorage bool     `yaml:"main_storage,omitempty"`
	Options       []string `yaml:"options,omitempty"`
}

func ReadConfig(configFile string) (c HermesConfig, err error) {
	_, err = os.Stat(configFile)
	if os.IsNotExist(err) {
		err = fmt.Errorf("file %s not found", configFile)
		return
	}

	yamlFile, err := ioutil.ReadFile(configFile)
	if err != nil {
		err = fmt.Errorf("read config file get error %v", err)
		return
	}
	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		err = fmt.Errorf("unmarshal config file get error %v", err)
		return
	}
	return
}
