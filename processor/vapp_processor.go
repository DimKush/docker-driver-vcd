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

// VAppProcessor creates a single instance vApp with VM instead
type VAppProcessor struct {
	cfg       ConfigProcessor
	vcdClient *client.VCloudClient
	VAppID    string
}

type CustomScriptConfigVAppProcessor struct {
	VAppName string
	SSHKey   string
	SSHUser  string
	UserData string
	InitData string
	Rke2     bool
}

func NewVAppProcessor(client *client.VCloudClient, cfg ConfigProcessor) Processor {
	return &VAppProcessor{
		cfg:       cfg,
		vcdClient: client,
	}
}

func (p *VAppProcessor) Create(customCfg interface{}) (*govcd.VApp, error) {
	log.Debugf("VAppProcessor.Create running with config: %+v", p.cfg)

	var err error

	defer func() {
		if err != nil {
			log.Debugf("VAppProcessor.cleanState reason ----> %v", err)
			if errDel := p.cleanState(); errDel != nil {
				log.Errorf("VAppProcessor.cleanState error: %v", errDel)
			}
		}
	}()

	// creates networks instances
	networks := make([]*types.OrgVDCNetwork, 0)
	networks = append(networks, p.vcdClient.Network.OrgVDCNetwork)

	// creates template vApp
	log.Debugf("VAppProcessor.Create creates new vApp and VM instead with single name %s", p.cfg.VAppName)

	// check if vApp by name already exists
	var vAppExists *govcd.VApp
	vAppExists, err = p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		if !errors.Is(err, govcd.ErrorEntityNotFound) {
			log.Errorf("VAppProcessor.Create.GetVAppByName error: %v", err)
			return nil, err
		}
	}

	if vAppExists != nil {
		return nil, fmt.Errorf("VAppProcessor.Create vApp with a same name already exists: %s", p.cfg.VAppName)
	}

	// create a new vApp
	vApp, err := p.vcdClient.VirtualDataCenter.CreateRawVApp(p.cfg.VAppName, "Container Host created with Docker Host by VAppProcessor")
	if err != nil {
		log.Errorf("VAppProcessor.Create.CreateRawVApp error: %v", err)
		return nil, err
	}

	taskNet, err := vApp.AddRAWNetworkConfig(networks)
	if err != nil {
		log.Errorf("VAppProcessor.Create.AddRAWNetworkConfig error: %v", err)
		return nil, err
	}

	err = taskNet.WaitTaskCompletion()
	if err != nil {
		log.Errorf("VAppProcessor.Create.WaitTaskCompletion error: %v", err)
		return nil, err
	}

	// create a new VM with a SAME name as vApp
	task, err := vApp.AddNewVM(
		p.cfg.VAppName,
		p.vcdClient.VAppTemplate,
		p.vcdClient.VAppTemplate.VAppTemplate.Children.VM[0].NetworkConnectionSection,
		true,
	)
	if err != nil {
		log.Errorf("VAppProcessor.Create.AddNewVM error: %v", err)
		return nil, err
	}

	// Wait for the creation to be completed
	err = task.WaitTaskCompletion()
	if err != nil {
		log.Errorf("VAppProcessor.Create.WaitTaskCompletion error: %v", err)
		return nil, err
	}

	// get vApp by name to check if it was created correctly
	vApp, err = p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VAppProcessor.Create.GetVAppByName error: %v", err)
		return nil, err
	}

	// get VM by name to check if it was created correctly
	virtualMachine, err := vApp.GetVMByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VAppProcessor.Create.GetVMByName error: %v", err)
		return nil, err
	}

	// Wait while VM is creating and powered off
	for {
		vApp, err = p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
		if err != nil {
			log.Errorf("VAppProcessor.Create.GetVAppByName error: %v", err)
			return nil, err
		}

		var vm *govcd.VM
		vm, err = vApp.GetVMByName(p.cfg.VAppName, true)
		if err != nil {
			log.Errorf("VAppProcessor.Create.GetVMByName error: %v", err)
			return nil, err
		}

		if vm.VM.VmSpecSection != nil {
			var status string
			status, err = vApp.GetStatus()
			if err != nil {
				log.Errorf("VAppProcessor.Create.GetStatus error: %v", err)
				return nil, err
			}

			log.Debugf("VAppProcessor.Create current vapp %s status: %s", p.cfg.VAppName, status)

			if status == "POWERED_OFF" {
				virtualMachine = vm
				break
			}
		}

		time.Sleep(time.Second * 1)
	}

	// set post settings for VM
	log.Debugf("VAppProcessor.Create vApp and vm: %s. Set post settings", p.cfg.VAppName)

	err = p.vmPostSettings(virtualMachine)
	if err != nil {
		log.Errorf("VAppProcessor.Create.vmPostSettings error: %v", err)
		return nil, err
	}

	// set custom configs if it's not empty
	if customCfg != nil {
		var guestSection types.GuestCustomizationSection
		guestSection, err = p.prepareCustomSectionForVM(*virtualMachine.VM.GuestCustomizationSection, customCfg)
		if err != nil {
			return nil, fmt.Errorf("VAppProcessor.Create.prepareCustomSectionForVM error: %w", err)
		}

		_, err = virtualMachine.SetGuestCustomizationSection(&guestSection)
		if err != nil {
			return nil, fmt.Errorf("VAppProcessor.Create.SetGuestCustomizationSection error: %w", err)
		}
	}

	if p.cfg.EdgeGateway != "" && p.cfg.PublicIP != "" {
		if p.cfg.VdcEdgeGateway != "" {
			var vdcGateway *govcd.Vdc
			vdcGateway, err = p.vcdClient.Org.GetVDCByName(p.cfg.VdcEdgeGateway, true)
			if err != nil {
				log.Errorf("VAppProcessor.Create.GetVDCByName error: %v", err)

				return nil, err
			}

			var edge *govcd.EdgeGateway
			edge, err = vdcGateway.GetEdgeGatewayByName(p.cfg.EdgeGateway, true)
			if err != nil {
				log.Errorf("VAppProcessor.Create.GetEdgeGatewayByName error: %v", err)

				return nil, err
			}

			log.Debugf("VAppProcessor.Create Creating NAT and Firewall Rules on %s...", p.cfg.EdgeGateway)

			var task1To1Map govcd.Task
			task1To1Map, err = edge.Create1to1Mapping(
				virtualMachine.VM.NetworkConnectionSection.NetworkConnection[0].IPAddress,
				p.cfg.PublicIP,
				p.cfg.VAppName,
			)
			if err != nil {
				log.Errorf("VAppProcessor.Create.Create1to1Mapping error: %v", err)

				return nil, err
			}

			err = task1To1Map.WaitTaskCompletion()
			if err != nil {
				log.Errorf("VAppProcessor.Create.WaitTaskCompletion error: %v", err)

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
				log.Errorf("VAppProcessor.Create.GetAdminOrgByName error: %v", err)

				return nil, err
			}

			var edge *govcd.NsxtEdgeGateway
			edge, err = adminOrg.GetNsxtEdgeGatewayByName(p.cfg.EdgeGateway)
			if err != nil {
				log.Errorf("VAppProcessor.Create.GetNsxtEdgeGatewayByName error: %v", err)

				return nil, err
			}

			_, err = edge.CreateNatRule(snatRuleDefinition)
			if err != nil {
				log.Errorf("VAppProcessor.Create.CreateNatRule error: %v", err)

				return nil, err
			}

			_, err = edge.CreateNatRule(dnatRuleDefinition)
			if err != nil {
				log.Errorf("VAppProcessor.Create.CreateNatRule error: %v", err)

				return nil, err
			}
		}
	}

	return vApp, nil
}

// VMPostSettings - post settings for VM after VM was created (CPU, Disk, Memory, custom scripts, etc...)

func (p *VAppProcessor) Remove() error {
	log.Debugf("VAppProcessor.Remove running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppById(p.cfg.VAppID, true)
	if err != nil {
		log.Errorf("VAppProcessor.Remove.GetVAppById error: %v", err)
		return err
	}

	log.Debugf("VAppProcessor.Remove found vApp with id %v and name %s", p.cfg.VAppID, p.cfg.VAppName)

	if p.cfg.EdgeGateway != "" && p.cfg.PublicIP != "" {
		log.Debugf("VAppProcessor.Remove delete network connection for %s", p.cfg.VAppName)

		if p.cfg.VdcEdgeGateway != "" {
			log.Debugf("VAppProcessor.Remove VdcEdgeGateway %s", p.cfg.EdgeGateway)

			vdcGateway, err := p.vcdClient.Org.GetVDCByName(p.cfg.VdcEdgeGateway, true)
			if err != nil {
				log.Errorf("VAppProcessor.Remove.GetVDCByName error: %v", err)
				return err
			}
			edge, err := vdcGateway.GetEdgeGatewayByName(p.cfg.EdgeGateway, true)
			if err != nil {
				log.Errorf("VAppProcessor.Remove.GetEdgeGatewayByName error: %v", err)
				return err
			}

			log.Debugf("VAppProcessor.Removing NAT and Firewall Rules on %s...", p.cfg.EdgeGateway)

			task, err := edge.Remove1to1Mapping(vApp.VApp.Children.VM[0].NetworkConnectionSection.NetworkConnection[0].IPAddress, p.cfg.PublicIP)
			if err != nil {
				return err
			}
			if err = task.WaitTaskCompletion(); err != nil {
				return err
			}
		} else {
			log.Debugf("VAppProcessor.Remove delete nat rules %s", p.cfg.VAppName)

			adminOrg, err := p.vcdClient.Client.GetAdminOrgByName(p.cfg.Org)
			edge, err := adminOrg.GetNsxtEdgeGatewayByName(p.cfg.EdgeGateway)

			dnat, err := edge.GetNatRuleByName(p.cfg.VAppName + "_dnat")
			if err != nil {
				return err
			}

			if errDel := dnat.Delete(); errDel != nil {
				log.Errorf("VAppProcessor.Remove.Delete dnat error: %v", errDel)
				return errDel
			}

			snat, err := edge.GetNatRuleByName(p.cfg.VAppName + "_snat")
			if err != nil {
				return err
			}
			if errDel := snat.Delete(); errDel != nil {
				log.Errorf("VAppProcessor.Remove.Delete snat error: %v", errDel)
				return err
			}
		}
	}

	log.Debugf("VAppProcessor.Remove %s get vApp name", p.cfg.VAppName)

	status, err := vApp.GetStatus()
	if err != nil {
		log.Errorf("VAppProcessor.Remove.GetStatus error: %v", err)
		return err
	}

	if status == "POWERED_ON" {
		// If it's powered on, power it off before deleting
		log.Debugf("VAppProcessor.Remove power it off %s...", p.cfg.VAppName)
		task, err := vApp.PowerOff()
		if err != nil {
			log.Errorf("VAppProcessor.Remove.PowerOff error: %v", err)
			return err
		}
		if err = task.WaitTaskCompletion(); err != nil {
			log.Errorf("VAppProcessor.Remove.WaitTaskCompletion error: %v", err)
			return err
		}
	}

	log.Debugf("VAppProcessor.Remove Undeploying %s", p.cfg.VAppName)

	task, err := vApp.Undeploy()
	if err != nil {
		log.Errorf("VAppProcessor.Remove.Undeploy error: %v", err)
		return err
	}

	if err = task.WaitTaskCompletion(); err != nil {
		log.Errorf("VAppProcessor.Remove.WaitTaskCompletion error: %v", err)
		return err
	}

	log.Debugf("VAppProcessor.Remove delete vapp %s", p.cfg.VAppName)

	task, err = vApp.Delete()
	if err != nil {
		log.Errorf("VAppProcessor.Remove.Delete error: %v", err)
		return err
	}

	if err = task.WaitTaskCompletion(); err != nil {
		log.Errorf("VAppProcessor.Remove.WaitTaskCompletion error: %v", err)
		return err
	}

	return nil
}

func (p *VAppProcessor) Stop() error {
	log.Debugf("VAppProcessor.Stop running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppById(p.cfg.VAppID, true)
	if err != nil {
		log.Errorf("VAppProcessor.Stop.GetVAppById error: %v", err)
		return err
	}

	task, errTask := vApp.Shutdown()
	if errTask != nil {
		log.Errorf("VAppProcessor.Stop.Shutdown error: %v", errTask)
		return errTask
	}

	if errWait := task.WaitTaskCompletion(); errTask != nil {
		log.Errorf("VAppProcessor.Stop.WaitTaskCompletion error: %v", errWait)
		return errWait
	}

	return nil
}

func (p *VAppProcessor) Kill() error {
	log.Debugf("VAppProcessor.Kill running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppById(p.cfg.VAppID, true)
	if err != nil {
		log.Errorf("VAppProcessor.Kill.GetVAppById error: %v", err)
		return err
	}

	task, errTask := vApp.PowerOff()
	if errTask != nil {
		log.Errorf("VAppProcessor.Kill.PowerOff error: %v", errTask)
		return errTask
	}

	if errWait := task.WaitTaskCompletion(); errWait != nil {
		log.Errorf("VAppProcessor.Kill.WaitTaskCompletion error: %v", errWait)
		return errWait
	}

	return nil
}

func (p *VAppProcessor) Restart() error {
	log.Debugf("VAppProcessor.Restart running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppById(p.cfg.VAppID, true)
	if err != nil {
		log.Errorf("VAppProcessor.Restart.GetVAppById error: %v", err)
		return err
	}

	task, err := vApp.Reset()
	if err != nil {
		log.Errorf("VAppProcessor.Restart.Reset error: %v", err)
		return err
	}

	if err = task.WaitTaskCompletion(); err != nil {
		log.Errorf("VAppProcessor.Restart.WaitTaskCompletion error: %v", err)
		return err
	}

	return nil
}

func (p *VAppProcessor) Start() error {
	log.Debugf("VAppProcessor.Start running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppById(p.cfg.VAppID, true)
	if err != nil {
		log.Errorf("VAppProcessor.Start.GetVAppById error: %v", err)
		return err
	}

	status, err := vApp.GetStatus()
	if err != nil {
		log.Errorf("VAppProcessor.Start.GetStatus error: %v", vApp)
		return err
	}

	log.Debugf("VAppProcessor.Start.GetStatus vapp %s status: %s", p.cfg.VAppName, status)

	if status == "POWERED_OFF" {
		log.Debugf("VAppProcessor.Start %s", p.cfg.VAppName)

		task, errOn := vApp.PowerOn()
		if errOn != nil {
			log.Errorf("VAppProcessor.Start.PowerOn error: %v", errOn)
			return errOn
		}

		if errTask := task.WaitTaskCompletion(); errTask != nil {
			log.Errorf("VAppProcessor.Start.WaitTaskCompletion error: %v", errTask)
			return errTask
		}
	}

	return nil
}

func (p *VAppProcessor) GetState() (state.State, error) {
	log.Debugf("VAppProcessor.GetState running with config: %+v", p.cfg)

	vApp, errApp := p.vcdClient.VirtualDataCenter.GetVAppById(p.cfg.VAppID, true)
	if errApp != nil {
		log.Errorf("VAppProcessor.GetState.GetVAppById error: %v", errApp)
		return state.None, errApp
	}

	status, errStatus := vApp.GetStatus()
	if errStatus != nil {
		log.Errorf("VAppProcessor.GetState.GetStatus error: %v", errStatus)
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

func (p *VAppProcessor) vmPostSettings(vm *govcd.VM) error {
	log.Debugf("VAppProcessor.vmPostSettings running with custom config: %+v", p.cfg)

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
		return fmt.Errorf("VAppProcessor.vmPostSettings.UpdateVmSpecSection error: %w", err)
	}

	return nil
}

func (p *VAppProcessor) prepareCustomSectionForVM(
	vmScript types.GuestCustomizationSection,
	customCfg interface{},
) (types.GuestCustomizationSection, error) {
	cfg, ok := customCfg.(CustomScriptConfigVAppProcessor)
	if !ok {
		return types.GuestCustomizationSection{}, fmt.Errorf("VAppProcessor.prepareCustomSectionForVM invalid config type: %T", cfg)
	}

	log.Debugf("VAppProcessor.prepareCustomSectionForVM running with custom config: %+v", cfg)

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
	scriptSh += "\nuseradd -m -d /home/" + cfg.SSHUser + " -s /bin/bash " + cfg.SSHUser + "\nmkdir -p /home/" + cfg.SSHUser + "/.ssh\nchmod 700 /home/" + cfg.SSHUser + "/.ssh\ntouch /home/" + cfg.SSHUser + "/.ssh/authorized_keys\nchmod 600 /home/" + cfg.SSHUser + "/.ssh/authorized_keys\necho \"" + strings.TrimSpace(cfg.SSHKey) + "\" > /home/" + cfg.SSHUser + "/.ssh/authorized_keys\necho \"" + cfg.SSHUser + "     ALL=(ALL) NOPASSWD:ALL\" >>  /etc/sudoers\nchown -R " + cfg.SSHUser + ". -R /home/" + cfg.SSHUser + "\n"

	if cfg.Rke2 {
		// if rke2
		readUserData, errRead := os.ReadFile(cfg.UserData)
		if errRead != nil {
			log.Errorf("VAppProcessor.prepareCustomSection.ReadFile error: %s", errRead)
			return types.GuestCustomizationSection{}, errRead
		}

		cloudInit := rancher.GetCloudInitRancher(string(readUserData))

		log.Debugf("VAppProcessor.prepareCustomSection ----> rke2: %v Generate /usr/local/custom_script/install.sh file", cfg.Rke2)

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

	section.CustomizationScript = scriptSh

	return section, nil
}

func (p *VAppProcessor) cleanState() error {
	log.Debugf("VAppProcessor.cleanState running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VAppProcessor.cleanState.GetVAppByName error: %v", err)
		return err
	}

	if p.cfg.EdgeGateway != "" && p.cfg.PublicIP != "" {
		if p.cfg.VdcEdgeGateway != "" {
			vdcGateway, err := p.vcdClient.Org.GetVDCByName(p.cfg.VdcEdgeGateway, true)
			if err != nil {
				log.Errorf("VAppProcessor.cleanState.GetVDCByName error: %v", err)
				return err
			}

			edge, err := vdcGateway.GetEdgeGatewayByName(p.cfg.EdgeGateway, true)
			if err != nil {
				log.Errorf("VAppProcessor.cleanState.GetEdgeGatewayByName error: %v", err)
				return err
			}

			log.Debugf("VAppProcessor.cleanState NAT and Firewall Rules on %s...", p.cfg.EdgeGateway)

			task, err := edge.Remove1to1Mapping(vApp.VApp.Children.VM[0].NetworkConnectionSection.NetworkConnection[0].IPAddress, p.cfg.PublicIP)
			if err != nil {
				return err
			}
			if err = task.WaitTaskCompletion(); err != nil {
				return err
			}
		} else {
			adminOrg, err := p.vcdClient.Client.GetAdminOrgByName(p.cfg.Org)
			edge, err := adminOrg.GetNsxtEdgeGatewayByName(p.cfg.EdgeGateway)

			dnat, err := edge.GetNatRuleByName(p.cfg.VAppName + "_dnat")
			if err != nil {
				return err
			}

			if errDel := dnat.Delete(); errDel != nil {
				log.Errorf("VAppProcessor.cleanState.Delete error: %v", errDel)
				return err
			}

			snat, err := edge.GetNatRuleByName(p.cfg.VAppName + "_snat")
			if err != nil {
				return err
			}
			if errDel := snat.Delete(); errDel != nil {
				log.Errorf("VAppProcessor.cleanState.GetNatRuleByName error: %v", errDel)
				return err
			}
		}
	}

	for {
		status, err := vApp.GetStatus()
		if err != nil {
			log.Errorf("VAppProcessor.cleanState.GetStatus error: %v", err)
			return err
		}

		if status == "UNRESOLVED" {
			log.Debugf("VAppProcessor.cleanState waiting for %s...", p.cfg.VAppName)
			time.Sleep(1 * time.Second)
			continue
		}

		if status != "POWERED_OFF" {
			log.Debugf("VAppProcessor.cleanState machine :%s status is %s. Power it off", p.cfg.VAppName, status)
			task, err := vApp.PowerOff()

			if err != nil {
				log.Errorf("VAppProcessor.cleanState.PowerOff error: %v", err)
				return err
			}

			if err = task.WaitTaskCompletion(); err != nil {
				log.Errorf("VAppProcessor.cleanState.PowerOff.WaitTaskCompletion error: %v", err)
				return err
			}
			break
		} else {
			log.Debugf("VAppProcessor.cleanState.Powered Off %s...", p.cfg.VAppName)
			break
		}
	}

	log.Debugf("VAppProcessor.cleanState.Delete %s...", p.cfg.VAppName)
	task, err := vApp.Delete()
	if err != nil {
		return err
	}

	if err = task.WaitTaskCompletion(); err != nil {
		log.Errorf("VAppProcessor.cleanState.WaitTaskCompletion after task error: %v", err)
		return err
	}

	log.Debugf("VAppProcessor.cleanState %s...", p.cfg.VAppName)

	return nil
}
