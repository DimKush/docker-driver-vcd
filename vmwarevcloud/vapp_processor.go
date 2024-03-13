package vmwarevcloud

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/docker/machine/libmachine/log"
	"github.com/vmware/go-vcloud-director/v2/govcd"
	"github.com/vmware/go-vcloud-director/v2/types/v56"
)

var ErrVAppAlreadyExists = errors.New("vApp already exists")

// VAppProcessor creates a single instance vApp with VM instead
type VAppProcessor struct {
	vAppName       string
	CPUCount       int
	Memory         int64
	Disk           int64
	EdgeGateway    string
	PublicIP       string
	VdcEdgeGateway string
	Org            string
	customCfg      interface{}
}

type customScriptConfigVAppProcessor struct {
	vAppName string
	sshKey   string
	sshUser  string
	userData string
	initData string
	rke2     bool
}

func NewVAppProcessor(vAppName string, sshKeyReady string, customCfg interface{}) Processor {
	return &VAppProcessor{
		vAppName:  vAppName,
		sshKey:    sshKeyReady,
		customCfg: customCfg,
	}
}

func (p *VAppProcessor) CreateVAppWithVM(vdcClient *VCloudClient) (*govcd.VApp, *govcd.VM, error) {
	log.Info("VAppProcessor.Create()")

	var err error

	defer func() {
		if err != nil {
			log.Infof("VAppProcessor.CleanState() reason ----> %v", err)
			if errDel := p.CleanState(vdcClient); errDel != nil {
				log.Errorf("VAppProcessor.Create().ClearError error: %v", errDel)
			}
		}
	}()

	// creates networks instances
	networks := make([]*types.OrgVDCNetwork, 0)
	networks = append(networks, vdcClient.network.OrgVDCNetwork)

	// creates template vApp
	log.Info("VAppProcessor.Create().VCloudClient Creates new vApp and VM instead with single name %s", p.vAppName)

	// check if vApp by name already exists
	vAppExists, errApp := vdcClient.virtualDataCenter.GetVAppByName(p.vAppName, true)
	if errApp != nil {
		if !errors.Is(errApp, govcd.ErrorEntityNotFound) {
			log.Errorf("VAppProcessor.Create().VCloudClient.GetVAppByName error: %v", errApp)
			return nil, nil, errApp
		}
	}

	if vAppExists != nil {
		return nil, nil, fmt.Errorf("vApp with a same name already exists: %s", cmd.vAppName)
	}

	// create a new vApp
	vApp, err := vdcClient.virtualDataCenter.CreateRawVApp(p.vAppName, "Container Host created with Docker Host by VAppProcessor")
	if err != nil {
		log.Errorf("VAppProcessor.Create().VCloudClient.CreateRawVApp error: %v", err)
		return nil, nil, err
	}

	taskNet, err := vApp.AddRAWNetworkConfig(networks)
	if err != nil {
		log.Errorf("VAppProcessor.Create.AddRAWNetworkConfig error: %v", err)
		return nil, nil, err
	}

	err = taskNet.WaitTaskCompletion()
	if err != nil {
		log.Errorf("VAppProcessor.CreateRawVApp.WaitTaskCompletion vdcClient.virtualDataCenter.ComposeVApp error: %v", err)
		return nil, nil, err
	}

	// create a new VM with a SAME name as vApp
	task, err := vApp.AddNewVM(
		p.vAppName,
		vdcClient.vAppTemplate,
		vdcClient.vAppTemplate.VAppTemplate.Children.VM[0].NetworkConnectionSection,
		true,
	)
	if err != nil {
		log.Errorf("VAppProcessor.Create.AddNewVM error: %v", err)
		return nil, nil, err
	}

	// Wait for the creation to be completed
	err = task.WaitTaskCompletion()
	if err != nil {
		log.Errorf("VAppProcessor.AddNewVM.WaitTaskCompletion  vdcClient.virtualDataCenter.ComposeVApp error: %v", err)
		return nil, nil, err
	}

	// get vApp by name to check if it was created correctly
	vApp, err = vdcClient.virtualDataCenter.GetVAppByName(p.vAppName, true)
	if err != nil {
		log.Errorf("VAppProcessor.Create().VCloudClient.GetVAppByName error: %v", err)
		return nil, nil, err
	}

	// get VM by name to check if it was created correctly
	virtualMachine, err := vApp.GetVMByName(p.vAppName, true)
	if err != nil {
		log.Errorf("VAppProcessor.Create().VCloudClient.GetVMByName error: %v", err)
		return nil, nil, err
	}

	log.Info("VAppProcessor.Create().VCloudClient Creates new vApp and VM instead with single name %s", p.vAppName)

	// Wait while VM is creating and powered off
	for {
		vApp, err = vdcClient.virtualDataCenter.GetVAppByName(p.vAppName, true)
		if err != nil {
			log.Errorf("VAppProcessor.Create.GetVAppByName error: with machine %d error: %v", p.vAppName, err)
			return nil, nil, err
		}

		var vm *govcd.VM
		vm, err = vApp.GetVMByName(p.vAppName, true)
		if err != nil {
			log.Errorf("VAppProcessor.Create.GetVMByName error: %v", err)
			return nil, nil, err
		}

		if virtualMachine.VM.VmSpecSection != nil {
			var status string

			status, err = vApp.GetStatus()
			if err != nil {
				log.Errorf("VAppProcessor.Create.GetStatus error: %v", err)
				return nil, nil, err
			}

			log.Infof("VAppProcessor.Create().VCloudClient waiting for vm %s created and powered off. Current status: %s", cmd.vAppName, status)

			if status == "POWERED_OFF" {
				virtualMachine = vm
				break
			}
		}

		time.Sleep(time.Second * 1)
	}

	// set post settings for VM
	log.Info("VAppProcessor.Create() vm was created and powered off. Set post-settings before run VM")
	err = p.vmPostSettings(virtualMachine)

	return nil
}

// VMPostSettings - post settings for VM after VM was created (CPU, Disk, Memory, custom scripts, etc...)
func (p *VAppProcessor) vmPostSettings(vm *govcd.VM) error {
	var numCPUsPtr *int

	// config VM
	cpuCount := p.CPUCount
	numCPUsPtr = &cpuCount

	vmSpecs := *vm.VM.VmSpecSection

	vmSpecs.NumCpus = numCPUsPtr
	vmSpecs.NumCoresPerSocket = numCPUsPtr
	vmSpecs.MemoryResourceMb.Configured = p.Memory
	vmSpecs.DiskSection.DiskSettings[0].SizeMb = p.Disk

	_, err := vm.UpdateVmSpecSection(&vmSpecs, vm.VM.Description)
	if err != nil {
		return fmt.Errorf("UpdateVmSpecSection error: %w", err)
	}

	// set custom configs if it's not empty
	if p.customCfg != nil {
		guestSection, errSection := p.prepareCustomSectionForVM(*vm.VM.GuestCustomizationSection)
		if errSection != nil {
			return fmt.Errorf("prepareCustomSectionForVM error: %w", errSection)
		}

		_, errSet := vm.SetGuestCustomizationSection(&guestSection)
		if errSet != nil {
			return fmt.Errorf("SetGuestCustomizationSection error: %w", errSet)
		}
	}

	return nil

}

func (p *VAppProcessor) prepareCustomSectionForVM(vmScript types.GuestCustomizationSection) (types.GuestCustomizationSection, error) {
	// TODO DON'T FORGET
	//sshKey, errSsh := d.createSSHKey()
	//if errSsh != nil {
	//	log.Errorf("prepareCustomSection.createSSHKey error: %s", errSsh)
	//	return types.GuestCustomizationSection{}, errSsh
	//}

	cfg, ok := p.customCfg.(*customScriptConfigVAppProcessor)
	if !ok {
		return types.GuestCustomizationSection{}, fmt.Errorf("invalid config type: %T", cfg)
	}

	var (
		section      types.GuestCustomizationSection
		adminEnabled bool
		scriptSh     string
	)

	section = vmScript

	section.ComputerName = cfg.vAppName
	section.AdminPasswordEnabled = &adminEnabled

	scriptSh = cfg.initData + "\n"
	// append ssh user to script
	scriptSh += "\nuseradd -m -d /home/" + cfg.sshUser + " -s /bin/bash " + cfg.sshUser + "\nmkdir -p /home/" + cfg.sshUser + "/.ssh\nchmod 700 /home/" + cfg.sshUser + "/.ssh\ntouch /home/" + cfg.sshUser + "/.ssh/authorized_keys\nchmod 600 /home/" + cfg.sshUser + "/.ssh/authorized_keys\nusermod -a -G sudo " + cfg.sshUser + "\necho \"" + strings.TrimSpace(cfg.sshKey) + "\" > /home/" + cfg.sshUser + "/.ssh/authorized_keys\necho \"" + cfg.sshUser + "     ALL=(ALL) NOPASSWD:ALL\" >>  /etc/sudoers\nchown -R " + cfg.sshUser + ":" + cfg.sshUser + " -R /home/" + cfg.sshUser + "\n"

	if cfg.rke2 {
		// if rke2
		readUserData, errRead := os.ReadFile(cfg.userData)
		if errRead != nil {
			log.Errorf("prepareCustomSection.ReadFile error: %s", errRead)
			return types.GuestCustomizationSection{}, errRead
		}

		cloudInit := getRancherCloudInit(string(readUserData))

		log.Infof("prepareCustomSection ----> rke2: %v Generate /usr/local/custom_script/install.sh file", cfg.rke2)

		// generate install.sh
		cloudInitWithQuotes := strings.Join([]string{"'", cloudInit, "'"}, "")
		scriptSh += "mkdir -p /usr/local/custom_script\n"
		scriptSh += "echo " + cloudInitWithQuotes + " | base64 -d | gunzip | sudo tee /usr/local/custom_script/install.sh\n"
		scriptSh += "nohup sh /usr/local/custom_script/install.sh > /dev/null 2>&1 &\n"
		scriptSh += "exit 0\n"
	} else {
		// if rke1
		scriptSh += cfg.userData
	}

	log.Infof("prepareCustomSection generate script ----> %s", scriptSh)

	section.CustomizationScript = scriptSh

	return section, nil
}

func (p *VAppProcessor) CleanState(vdcClient *VCloudClient) error {
	log.Info("VAppProcessor.CleanState() running")

	vApp, err := vdcClient.virtualDataCenter.GetVAppByName(p.vAppName, true)
	if err != nil {
		log.Errorf("VAppProcessor.CleanState().GetVAppByName error: %v", err)
		return err
	}

	if p.EdgeGateway != "" && p.PublicIP != "" {
		if p.VdcEdgeGateway != "" {
			vdcGateway, err := vdcClient.org.GetVDCByName(p.VdcEdgeGateway, true)
			if err != nil {
				log.Errorf("VAppProcessor.Remove.GetVDCByName error: %v", err)
				return err
			}
			edge, err := vdcGateway.GetEdgeGatewayByName(p.EdgeGateway, true)
			if err != nil {
				log.Errorf("VAppProcessor.Remove.GetEdgeGatewayByName error: %v", err)
				return err
			}

			log.Infof("VAppProcessor.Removing NAT and Firewall Rules on %s...", p.EdgeGateway)

			task, err := edge.Remove1to1Mapping(vApp.VApp.Children.VM[0].NetworkConnectionSection.NetworkConnection[0].IPAddress, p.PublicIP)
			if err != nil {
				return err
			}
			if err = task.WaitTaskCompletion(); err != nil {
				return err
			}
		} else {
			adminOrg, err := vdcClient.client.GetAdminOrgByName(p.Org)
			edge, err := adminOrg.GetNsxtEdgeGatewayByName(p.EdgeGateway)

			dnat, err := edge.GetNatRuleByName(p.vAppName + "_dnat")
			if err != nil {
				return err
			}

			if errDel := dnat.Delete(); errDel != nil {
				log.Errorf("VAppProcessor.Remove.Delete dnat error: %v", errDel)
				return err
			}

			snat, err := edge.GetNatRuleByName(p.vAppName + "_snat")
			if err != nil {
				return err
			}
			if errDel := snat.Delete(); errDel != nil {
				log.Errorf("VAppProcessor.Remove.Delete snat error: %v", errDel)
				return err
			}
		}
	}

	for {
		status, err := vApp.GetStatus()
		if err != nil {
			log.Errorf("VAppProcessor.Remove.GetStatus error: %v", err)
			return err
		}

		if status == "UNRESOLVED" {
			log.Infof("VAppProcessor.Remove.Unresolved waiting for %s...", p.vAppName)
			time.Sleep(1 * time.Second)
			continue
		}

		if status != "POWERED_OFF" {
			log.Infof("VAppProcessor.Remove machine :%s status is %s. Power it off", p.vAppName, status)
			task, err := vApp.PowerOff()

			if err != nil {
				log.Errorf("VAppProcessor.Remove.PowerOff error: %v", err)
				return err
			}

			if err = task.WaitTaskCompletion(); err != nil {
				log.Errorf("VAppProcessor.Remove.PowerOff.WaitTaskCompletion error: %v", err)
				return err
			}
			break
		} else {
			log.Infof("VAppProcessor.Remove.Powered Off %s...", p.vAppName)
			break
		}
	}

	log.Infof("VAppProcessor.Remove.Delete %s...", p.vAppName)
	task, err := vApp.Delete()
	if err != nil {
		return err
	}

	if err = task.WaitTaskCompletion(); err != nil {
		log.Errorf("Remove.Undeploy.WaitTaskCompletion after undeploy error: %v", err)
		return err
	}

	log.Infof("Remove.Deleting %s...", p.vAppName)

	return nil
}
