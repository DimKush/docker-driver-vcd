package processor

import (
	"github.com/vmware/go-vcloud-director/v2/govcd"
)

// Processor represents an interface to work with VMWare vCloud Director
// Use a special processor implementation to work with different cases
//
// VMProcessor - if you need to work with one vApp and create VM in this vApp (vapp-name flag)
//
// VAppProcessor - if you need to work with one vApp and one VM in VApp (1 to 1). vapp-name is not taken into account.
// VAppProcessor creates Vapp (if not exists) and VM in VApp with same name

type Processor interface {
	Create(customCfg interface{}) (*govcd.VApp, error)
	Remove() error
	Stop() error
	Kill() error
	CleanState() error
	vmPostSettings(vm *govcd.VM) error
	Restart() error
	Start() error
}
