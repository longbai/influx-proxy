package backend

import (
	"gopkg.in/yaml.v2"
	"os"
)

type FileConfigSource struct {
	Node      NodeConfig                `yaml:"node"`
	Backends  map[string]*BackendConfig `yaml:"backends"`
	KeyRouter map[string][]string       `yaml:"key_router"`
	KeyIgnore []string                  `yaml:"key_ignore"`
}

func NewFileConfigSource(path string) (f *FileConfigSource, err error) {
	var fc FileConfigSource
	file, err := os.Open(path)
	if err != nil {
		return
	}
	dec := yaml.NewDecoder(file)
	err = dec.Decode(&fc)
	return &fc, nil
}

func (rcs *FileConfigSource) LoadNode() (nodeCfg NodeConfig, err error) {
	return rcs.Node, nil
}

func (rcs *FileConfigSource) LoadAllBackends() (backends map[string]*BackendConfig, err error) {
	return rcs.Backends, nil
}

func (rcs *FileConfigSource) LoadBackend(name string) (cfg *BackendConfig, err error) {
	return rcs.Backends[name], nil
}

func (rcs *FileConfigSource) LoadMeasurements() (m_map map[string][]string, err error) {
	return rcs.KeyRouter, nil
}
