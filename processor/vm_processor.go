package processor

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/DimKush/docker-driver-vcd/client"
	"github.com/DimKush/docker-driver-vcd/rancher"
	"github.com/docker/machine/libmachine/log"
	"github.com/docker/machine/libmachine/state"
	"github.com/vmware/go-vcloud-director/v2/govcd"
	"github.com/vmware/go-vcloud-director/v2/types/v56"
)

// VMProcessor creates a single instance vApp with VM instead
type VMProcessor struct {
	cfg       ConfigProcessor
	vcdClient *client.VCloudClient
	VAppID    string
}
type CustomScriptConfigVMProcessor struct {
	VAppName    string
	MachineName string
	SSHKey      string
	SSHUser     string
	UserData    string
	InitData    string
	Rke2        bool
}

func NewVMProcessor(client *client.VCloudClient, cfg ConfigProcessor) Processor {
	return &VMProcessor{
		cfg:       cfg,
		vcdClient: client,
	}
}

func (p *VMProcessor) checkVAppExistsAndCreateIfNot() (*govcd.VApp, error) {
	log.Infof("VMProcessor.checkVAppExistsAndCreateIfNot() running with config: %+v", p.cfg)

	vAppExist, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		if !errors.Is(err, govcd.ErrorEntityNotFound) {
			log.Errorf("VMProcessor.Create().checkVAppExistsAndCreateIfNot.VCloudClient.GetVAppByName error: %v", err)
			return nil, err
		}
	}

	// if exists, do nothing
	if vAppExist != nil {
		return vAppExist, nil
	}

	log.Infof("VMProcessor.Create.checkVAppExistsAndCreateIfNot VApp %s doesn't exist. Creates new vApp", p.cfg.VAppName)

	// creates networks instances
	networks := make([]*types.OrgVDCNetwork, 0)
	networks = append(networks, p.vcdClient.Network.OrgVDCNetwork)

	// create a new vApp
	vApp, err := p.vcdClient.VirtualDataCenter.CreateRawVApp(p.cfg.VAppName, "Container Host created with Docker Host by VMProcessor")
	if err != nil {
		log.Errorf("VMProcessor.Create.checkVAppExistsAndCreateIfNot.VCloudClient.CreateRawVApp error: %v", err)
		return nil, err
	}

	taskNet, err := vApp.AddRAWNetworkConfig(networks)
	if err != nil {
		log.Errorf("VMProcessor.Create.checkVAppExistsAndCreateIfNot.AddRAWNetworkConfig error: %v", err)
		return nil, err
	}

	err = taskNet.WaitTaskCompletion()
	if err != nil {
		log.Errorf("VMProcessor.checkVAppExistsAndCreateIfNot.WaitTaskCompletion p.vcdClient.virtualDataCenter.ComposeVApp error: %v", err)
		return nil, err
	}

	// wait until vApp will be ready
	for {
		status, errStatus := vApp.GetStatus()
		if errStatus != nil {
			log.Errorf("VMProcessor.Create.checkVAppExistsAndCreateIfNot.GetStatus error: %v", errStatus)
			return nil, err
		}

		log.Infof("VMProcessor.Create.checkVAppExistsAndCreateIfNot.GetStatus status: %s", status)
		if status != "RESOLVED" {
			// wait until VApp will be ready
			time.Sleep(time.Second * 2)
			continue
		} else {
			break
		}
	}

	return vApp, nil
}

func (p *VMProcessor) Create(customCfg interface{}) (*govcd.VApp, error) {
	log.Infof("VMProcessor.Create() running with config: %+v", p.cfg)

	vApp, errVApp := p.checkVAppExistsAndCreateIfNot()
	if errVApp != nil {
		log.Errorf("VMProcessor.Create().checkVAppExistsAndCreateIfNot error: %v", errVApp)
		return nil, errVApp
	}

	var err error

	defer func() {
		if err != nil {
			log.Infof("VMProcessor.CleanState() reason ----> %v", err)
			if errDel := p.CleanState(); errDel != nil {
				log.Errorf("VMProcessor.Create().ClearError error: %v", errDel)
			}
		}
	}()

	// creates template vApp
	log.Infof("VMProcessor.Create() Creates new VM %s instead vApp %s", p.cfg.VMachineName, p.cfg.VAppName)

	// check if VM by name exists
	vmExists, errExists := vApp.GetVMByName(p.cfg.VMachineName, true)
	if errExists != nil {
		if !errors.Is(errExists, govcd.ErrorEntityNotFound) {
			log.Errorf("VMProcessor.Create().GetVMByName error: %v", errExists)
			err = errExists

			return nil, err
		}
	}

	if vmExists != nil {
		return nil, fmt.Errorf("VMProcessor.Create().GetVMByName VM %s already exists in vApp: %s", p.cfg.VMachineName, p.cfg.VAppName)
	}

	// create a new VM in vApp
	task, err := vApp.AddNewVM(
		p.cfg.VMachineName,
		p.vcdClient.VAppTemplate,
		p.vcdClient.VAppTemplate.VAppTemplate.Children.VM[0].NetworkConnectionSection,
		true,
	)
	if err != nil {
		log.Errorf("VMProcessor.Create.AddNewVM error: %v", err)
		return nil, err
	}

	// Wait for the creation to be completed
	err = task.WaitTaskCompletion()
	if err != nil {
		log.Errorf("VMProcessor.AddNewVM.WaitTaskCompletion  p.vcdClient.virtualDataCenter.ComposeVApp error: %v", err)
		return nil, err
	}

	// get VM by name to check if it was created correctly
	virtualMachine, err := vApp.GetVMByName(p.cfg.VMachineName, true)
	if err != nil {
		log.Errorf("VMProcessor.Create().VCloudClient.GetVMByName error: %v", err)
		return nil, err
	}

	// Wait while VM is creating and powered off
	for {
		var vm *govcd.VM
		vm, err = vApp.GetVMByName(p.cfg.VMachineName, true)
		if err != nil {
			log.Errorf("VMProcessor.Create.GetVMByName error: %v", err)
			return nil, err
		}

		if vm.VM.VmSpecSection != nil {
			var status string
			status, err = vm.GetStatus()
			if err != nil {
				log.Errorf("VMProcessor.Create.GetStatus error: %v", err)
				return nil, err
			}

			log.Infof("VMProcessor.Create().VCloudClient waiting for vm %s created and powered off. Current status: %s", p.cfg.VAppName, status)

			if status == "POWERED_OFF" {
				virtualMachine = vm
				break
			}
		}

		time.Sleep(time.Second * 1)
	}

	// set post settings for VM
	log.Infof("VMProcessor.Create() vm was created and powered off. Set post-settings before run VM")
	err = p.vmPostSettings(virtualMachine)
	if err != nil {
		log.Errorf("VMProcessor.Create().vmPostSettings error: %v", err)
		return nil, err
	}

	// set custom configs if it's not empty
	if customCfg != nil {
		var guestSection types.GuestCustomizationSection

		guestSection, err = p.prepareCustomSectionForVM(*virtualMachine.VM.GuestCustomizationSection, customCfg)
		if err != nil {
			return nil, fmt.Errorf("prepareCustomSectionForVM error: %w", err)
		}

		_, err = virtualMachine.SetGuestCustomizationSection(&guestSection)
		if err != nil {
			return nil, fmt.Errorf("SetGuestCustomizationSection error: %w", err)
		}
	}

	if p.cfg.EdgeGateway != "" && p.cfg.PublicIP != "" {
		if p.cfg.VdcEdgeGateway != "" {
			var vdcGateway *govcd.Vdc
			vdcGateway, err = p.vcdClient.Org.GetVDCByName(p.cfg.VdcEdgeGateway, true)
			if err != nil {
				log.Errorf("VMProcessor.Create().GetVDCByName error: %v", err)

				return nil, err
			}

			var edge *govcd.EdgeGateway
			edge, err = vdcGateway.GetEdgeGatewayByName(p.cfg.EdgeGateway, true)
			if err != nil {
				log.Errorf("VMProcessor.Create().GetEdgeGatewayByName error: %v", err)

				return nil, err
			}

			log.Infof("VMProcessor Creating NAT and Firewall Rules on %s...", p.cfg.EdgeGateway)

			var task1To1Map govcd.Task
			task1To1Map, err = edge.Create1to1Mapping(
				virtualMachine.VM.NetworkConnectionSection.NetworkConnection[0].IPAddress,
				p.cfg.PublicIP,
				p.cfg.VAppName,
			)
			if err != nil {
				log.Errorf("VMProcessor.Create.Create1to1Mapping error: %v", err)

				return nil, err
			}

			err = task1To1Map.WaitTaskCompletion()
			if err != nil {
				log.Errorf("VMProcessor.Create.WaitTaskCompletion.WaitTaskCompletion error: %v", err)

				return nil, err
			}
		} else {
			snatRuleDefinition := &types.NsxtNatRule{
				Name:              p.cfg.VAppName + "_snat",
				Description:       p.cfg.VAppName,
				Enabled:           true,
				RuleType:          types.NsxtNatRuleTypeSnat,
				ExternalAddresses: virtualMachine.VM.NetworkConnectionSection.NetworkConnection[0].IPAddress,
				InternalAddresses: p.cfg.PublicIP,
				FirewallMatch:     types.NsxtNatRuleFirewallMatchBypass,
			}

			dnatRuleDefinition := &types.NsxtNatRule{
				Name:              p.cfg.VAppName + "_dnat",
				Description:       p.cfg.VAppName,
				Enabled:           true,
				RuleType:          types.NsxtNatRuleTypeDnat,
				ExternalAddresses: p.cfg.PublicIP,
				InternalAddresses: virtualMachine.VM.NetworkConnectionSection.NetworkConnection[0].IPAddress,
				FirewallMatch:     types.NsxtNatRuleFirewallMatchBypass,
			}

			var adminOrg *govcd.AdminOrg
			adminOrg, err = p.vcdClient.Client.GetAdminOrgByName(p.cfg.Org)
			if err != nil {
				log.Errorf("VMProcessor.Create.GetAdminOrgByName error: %v", err)

				return nil, err
			}

			var edge *govcd.NsxtEdgeGateway
			edge, err = adminOrg.GetNsxtEdgeGatewayByName(p.cfg.EdgeGateway)
			if err != nil {
				log.Errorf("VMProcessor.Create.GetNsxtEdgeGatewayByName error: %v", err)

				return nil, err
			}

			_, err = edge.CreateNatRule(snatRuleDefinition)
			if err != nil {
				log.Errorf("VMProcessor.Create.CreateNatRule error: %v", err)

				return nil, err
			}

			_, err = edge.CreateNatRule(dnatRuleDefinition)
			if err != nil {
				log.Errorf("VMProcessor.Create.CreateNatRule error: %v", err)

				return nil, err
			}
		}
	}

	return vApp, nil
}

func (p *VMProcessor) Remove() error {
	log.Infof("VMProcessor.Remove() running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VMProcessor.Remove.GetVAppByName error: %v", err)
		return err
	}

	virtualMachine, err := vApp.GetVMByName(p.cfg.VMachineName, true)
	if err != nil {
		log.Errorf("VMProcessor.Remove.GetVMByName error: %v", err)
		return err
	}

	// If it's powered on, power it off before deleting
	log.Infof("VMProcessor.Remove() power it off %s...", p.cfg.VAppName)
	task, errTask := virtualMachine.Undeploy()
	if errTask != nil {
		log.Errorf("VMProcessor.Remove.PowerOff error: %v", errTask)
		return errTask
	}

	if err = task.WaitTaskCompletion(); err != nil {
		log.Errorf("VMProcessor.Remove.WaitTaskCompletion error: %v", err)
		return err
	}

	log.Infof("VMProcessor.Remove() Deleting VM %s in app: %s", p.cfg.VMachineName, p.cfg.VAppName)

	err = virtualMachine.Delete()
	if err != nil {
		log.Errorf("VMProcessor.Remove.Delete error: %v", err)
		return err
	}

	return nil
}

func (p *VMProcessor) Stop() error {
	log.Infof("VMProcessor.Stop() running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VMProcessor.Stop.getVDCApp error: %v", err)
		return err
	}

	virtualMachine, err := vApp.GetVMByName(p.cfg.VMachineName, true)
	if err != nil {
		log.Errorf("VMProcessor.Stop.GetVMByName error: %v", err)
		return err
	}

	task, err := virtualMachine.PowerOff()
	if err != nil {
		log.Errorf("VMProcessor.Stop.PowerOff error: %v", err)
		return err
	}

	if errWait := task.WaitTaskCompletion(); errWait != nil {
		log.Errorf("VMProcessor.Stop.WaitTaskCompletion error: %v", errWait)
		return errWait
	}

	return nil
}

func (p *VMProcessor) Kill() error {
	log.Infof("VMProcessor.Kill() running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VMProcessor.Kill.GetVAppByName error: %v", err)
		return err
	}

	virtualMachine, err := vApp.GetVMByName(p.cfg.VMachineName, true)
	if err != nil {
		log.Errorf("VMProcessor.Kill.GetVMByName error: %v", err)
		return err
	}

	task, err := virtualMachine.PowerOff()
	if err != nil {
		log.Errorf("VMProcessor.Kill.PowerOff error: %v", err)
		return err
	}

	if errWait := task.WaitTaskCompletion(); errWait != nil {
		log.Errorf("VMProcessor.Kill.WaitTaskCompletion error: %v", errWait)
		return errWait
	}

	err = virtualMachine.Delete()
	if err != nil {
		log.Errorf("VMProcessor.Kill.Delete error: %v", err)
		return err
	}

	return nil
}

func (p *VMProcessor) Restart() error {
	log.Infof("VMProcessor.Restart() running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VMProcessor.Restart.GetVAppByName error: %v", err)
		return err
	}

	virtualMachine, err := vApp.GetVMByName(p.cfg.VMachineName, true)
	if err != nil {
		log.Errorf("VMProcessor.Kill.GetVMByName error: %v", err)
		return err
	}

	task, err := virtualMachine.PowerOff()
	if err != nil {
		log.Errorf("VMProcessor.Restart.Reset error: %v", err)
		return err
	}

	if err = task.WaitTaskCompletion(); err != nil {
		log.Errorf("VMProcessor.Restart.WaitTaskCompletion error: %v", err)
		return err
	}

	// wait while vm powered off

	for {
		vm, errName := vApp.GetVMByName(p.cfg.VMachineName, true)
		if errName != nil {
			log.Errorf("VMProcessor.Kill.GetVMByName error: %v", errName)
			return errName
		}

		status, err := vm.GetStatus()
		if err != nil {
			log.Errorf("VMProcessor.Restart.GetStatus error: %v", err)
			return err
		}

		log.Infof("VMProcessor.Restart.GetStatus with VM : %s current status :%s", p.cfg.VMachineName, status)

		if status == "POWERED_OFF" {
			virtualMachine = vm
			break
		} else {
			time.Sleep(2 * time.Second)
		}
	}

	task, err = virtualMachine.PowerOn()
	if err != nil {
		log.Errorf("VMProcessor.Restart.Reset error: %v", err)
		return err
	}

	if err = task.WaitTaskCompletion(); err != nil {
		log.Errorf("VMProcessor.Restart.WaitTaskCompletion error: %v", err)
		return err
	}

	return nil
}

func (p *VMProcessor) Start() error {
	log.Infof("VMProcessor.Start() running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VMProcessor.Start.GetVAppByName error: %v", err)
		return err
	}

	virtualMachine, err := vApp.GetVMByName(p.cfg.VMachineName, true)
	if err != nil {
		log.Errorf("VMProcessor.Start.GetVMByName error: %v", err)
		return err
	}

	status, err := virtualMachine.GetStatus()
	if err != nil {
		log.Errorf("VMProcessor.Start.getVcdStatus.GetStatus error: %v", vApp)
		return err
	}

	log.Infof("VMProcessor.Start.GetStatus current status :%s", status)

	if status == "POWERED_OFF" {
		log.Infof("VMProcessor.Start.VCloudClient Start machine %s", p.cfg.VAppName)
		task, errOn := virtualMachine.PowerOn()
		if errOn != nil {
			log.Errorf("VMProcessor.Start.PowerOn error: %v", errOn)
			return errOn
		}

		if errTask := task.WaitTaskCompletion(); errTask != nil {
			log.Errorf("VMProcessor.Start.WaitTaskCompletion error: %v", errTask)
			return errTask
		}
	}

	return nil
}

func (p *VMProcessor) CleanState() error {
	log.Infof("VMProcessor.CleanState() running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VMProcessor.CleanState().GetVAppByName error: %v", err)
		return err
	}

	virtualMachine, err := vApp.GetVMByName(p.cfg.VMachineName, true)
	if err != nil {
		log.Errorf("VMProcessor.CleanState().GetVMByName error: %v", err)
		return err
	}

	for {
		status, err := virtualMachine.GetStatus()
		if err != nil {
			log.Errorf("VMProcessor.Remove.GetStatus error: %v", err)
			return err
		}

		if status == "UNRESOLVED" {
			log.Infof("VMProcessor.Remove.Unresolved waiting for %s...", p.cfg.VAppName)
			time.Sleep(1 * time.Second)
			continue
		}

		if status != "POWERED_OFF" {
			log.Infof("VMProcessor.Remove machine :%s status is %s. Power it off", p.cfg.VAppName, status)
			task, err := virtualMachine.PowerOff()

			if err != nil {
				log.Errorf("VMProcessor.Remove.PowerOff error: %v", err)
				return err
			}

			if err = task.WaitTaskCompletion(); err != nil {
				log.Errorf("VMProcessor.Remove.PowerOff.WaitTaskCompletion error: %v", err)
				return err
			}
			break
		} else {
			log.Infof("VMProcessor.Remove.Powered Off %s...", p.cfg.VMachineName)
			break
		}
	}

	err = virtualMachine.Delete()
	if err != nil {
		log.Errorf("VMProcessor.Remove.Undeploy.WaitTaskCompletion after undeploy error: %v", err)
		return err
	}

	log.Infof("VMProcessor.Remove.Deleting %s...", p.cfg.VMachineName)

	return nil
}

func (p *VMProcessor) vmPostSettings(vm *govcd.VM) error {
	log.Infof("VMProcessor.vmPostSettings() running with custom config: %+v", p.cfg)

	var numCPUsPtr *int

	// config VM
	cpuCount := p.cfg.CPUCount
	numCPUsPtr = &cpuCount

	vmSpecs := *vm.VM.VmSpecSection

	vmSpecs.NumCpus = numCPUsPtr
	vmSpecs.NumCoresPerSocket = numCPUsPtr
	vmSpecs.MemoryResourceMb.Configured = p.cfg.MemorySize
	vmSpecs.DiskSection.DiskSettings[0].SizeMb = p.cfg.DiskSize

	_, err := vm.UpdateVmSpecSection(&vmSpecs, vm.VM.Description)
	if err != nil {
		return fmt.Errorf("UpdateVmSpecSection error: %w", err)
	}

	return nil
}

func (p *VMProcessor) GetState() (state.State, error) {
	log.Infof("VMProcessor.GetState() running with config: %+v", p.cfg)

	vApp, errApp := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppID, true)
	if errApp != nil {
		log.Errorf("GetState.getVcdStatus.GetStatus error: %v", errApp)
		return state.None, errApp
	}

	vm, err := vApp.GetVMByName(p.cfg.VMachineName, true)
	if err != nil {
		log.Errorf("GetState.getVcdStatus.GetStatus error: %v", err)
		return state.None, err
	}

	status, errStatus := vm.GetStatus()
	if errStatus != nil {
		log.Errorf("GetState.getVcdStatus.GetStatus error: %v", errStatus)
		return state.None, errStatus
	}

	switch status {
	case "POWERED_ON":
		return state.Running, nil
	case "POWERED_OFF":
		return state.Stopped, nil
	}

	return state.None, nil
}

func (p *VMProcessor) prepareCustomSectionForVM(
	vmScript types.GuestCustomizationSection,
	customCfg interface{},
) (types.GuestCustomizationSection, error) {
	cfg, ok := customCfg.(CustomScriptConfigVMProcessor)
	if !ok {
		return types.GuestCustomizationSection{}, fmt.Errorf("invalid config type: %T", cfg)
	}

	log.Infof("prepareCustomSectionForVM() running with custom config: %+v", cfg)

	var (
		section      types.GuestCustomizationSection
		adminEnabled bool
		scriptSh     string
	)

	section = vmScript

	section.ComputerName = cfg.VAppName
	section.AdminPasswordEnabled = &adminEnabled

	scriptSh = cfg.InitData + "\n"
	// append ssh user to script
	scriptSh += "\nuseradd -m -d /home/" + cfg.SSHUser + " -s /bin/bash " + cfg.SSHUser + "\nmkdir -p /home/" + cfg.SSHUser + "/.ssh\nchmod 700 /home/" + cfg.SSHUser + "/.ssh\ntouch /home/" + cfg.SSHUser + "/.ssh/authorized_keys\nchmod 600 /home/" + cfg.SSHUser + "/.ssh/authorized_keys\nusermod -a -G sudo " + cfg.SSHUser + "\necho \"" + strings.TrimSpace(cfg.SSHKey) + "\" > /home/" + cfg.SSHUser + "/.ssh/authorized_keys\necho \"" + cfg.SSHUser + "     ALL=(ALL) NOPASSWD:ALL\" >>  /etc/sudoers\nchown -R " + cfg.SSHUser + ":" + cfg.SSHUser + " -R /home/" + cfg.SSHUser + "\n"

	if cfg.Rke2 {
		// if rke2
		readUserData, errRead := os.ReadFile(cfg.UserData)
		if errRead != nil {
			log.Errorf("prepareCustomSection.ReadFile error: %s", errRead)
			return types.GuestCustomizationSection{}, errRead
		}

		cloudInit := rancher.GetCloudInitRancher(string(readUserData))

		log.Infof("prepareCustomSection ----> rke2: %v Generate /usr/local/custom_script/install.sh file", cfg.Rke2)

		// generate install.sh
		cloudInitWithQuotes := strings.Join([]string{"'", cloudInit, "'"}, "")
		scriptSh += "mkdir -p /usr/local/custom_script\n"
		scriptSh += "echo " + cloudInitWithQuotes + " | base64 -d | gunzip | sudo tee /usr/local/custom_script/install.sh\n"
		scriptSh += "nohup sh /usr/local/custom_script/install.sh > /dev/null 2>&1 &\n"
		scriptSh += "exit 0\n"
	} else {
		// if rke1
		scriptSh += cfg.UserData
	}

	log.Infof("prepareCustomSection generate script ----> %s", scriptSh)

	section.CustomizationScript = scriptSh

	return section, nil
}
