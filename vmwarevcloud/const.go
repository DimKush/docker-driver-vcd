package vmwarevcloud

import (
	"github.com/vmware/go-vcloud-director/v2/types/v56"
)

const (
	defaultCatalog                 = "Public Catalog"
	defaultCatalogItem             = "Ubuntu Server 12.04 LTS (amd64 20150127)"
	defaultCpus                    = 2
	defaultMemory                  = 2048
	defaultDisk                    = 20480
	defaultSSHPort                 = 22
	defaultDockerPort              = 2376
	defaultInsecure                = false
	defaultRke2                    = false
	defaultSSHUser                 = "docker"
	defaultAdapterType             = ""
	defaultIPAddressAllocationMode = types.IPAllocationModeDHCP
)
