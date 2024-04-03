package processor

import (
	"github.com/docker/machine/libmachine/state"
	"github.com/vmware/go-vcloud-director/v2/govcd"
)

// Processor represents an interface to work with VMWare vCloud Director
// Use a special processor implementation to work with different cases
//
// VMProcessor - if you need to work with one vApp and create VM in this vApp (vapp-name flag)
// If VApp doesn't exist, it will be created. If exists - VM will be added in this vApp
//
// VAppProcessor - if you need to work with one vApp and one VM in VApp (1 to 1). vapp-name is not taken into account.
// VAppProcessor creates Vapp (if not exists) and VM in VApp with same name

type Processor interface {
	Create(customCfg interface{}) (*govcd.VApp, error)
	Remove() error
	Stop() error
	Kill() error
	vmPostSettings(vm *govcd.VM) error
	Restart() error
	Start() error
	GetState() (state.State, error)
	cleanState() error
}

type ConfigProcessor struct {
	VAppName       string
	VMachineName   string
	CPUCount       int
	MemorySize     int64
	DiskSize       int64
	EdgeGateway    string
	PublicIP       string
	VdcEdgeGateway string
	Org            string
	VAppID         string
	VMachineID     string
}
