package vmwarevcloud

import (
	"github.com/vmware/go-vcloud-director/v2/govcd"
	"github.com/vmware/go-vcloud-director/v2/types/v56"
)

// Processor represents an interface to work with VMWare vCloud Director
// Use a special processor implementation to work with different cases
//
// VMProcessor - if you need to work with one vApp and create VM in this vApp (vapp-name flag)
//
// VAppProcessor - if you need to work with one vApp and one VM in VApp (1 to 1). vapp-name is not taken into account.
// VAppProcessor creates Vapp and VM in VApp with same name

type Processor interface {
	CreateVAppWithVM(vdcClient *VCloudClient) (*govcd.VApp, *govcd.VM, error)
	CleanState(vdcClient *VCloudClient) error
	vmPostSettings(vm *govcd.VM) error
	prepareCustomSectionForVM(vmScript types.GuestCustomizationSection) (types.GuestCustomizationSection, error)
}
