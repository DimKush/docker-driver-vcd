package rancher

import (
	"github.com/docker/machine/libmachine/log"
	"gopkg.in/yaml.v2"
)

type CloudInitRancher struct {
	Runcmd     []string `yaml:"runcmd"`
	WriteFiles []struct {
		Content     string `yaml:"content"`
		Encoding    string `yaml:"encoding"`
		Path        string `yaml:"path"`
		Permissions string `yaml:"permissions"`
	} `yaml:"write_files"`
}

func GetCloudInitRancher(s string) string {
	out := CloudInitRancher{}
	err := yaml.Unmarshal([]byte(s), &out)
	if err != nil {
		log.Debugf("Unmarshal: %v", err)
	}

	for _, entry := range out.WriteFiles {
		return entry.Content
	}

	return ""
}
