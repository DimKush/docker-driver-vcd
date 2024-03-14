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
		//VAppID:    conf.VAppID,
	}
}

func (p *VAppProcessor) Create(customCfg interface{}) (*govcd.VApp, error) {
	log.Info("VAppProcessor.Create()")

	var err error

	defer func() {
		if err != nil {
			log.Infof("VAppProcessor.CleanState() reason ----> %v", err)
			if errDel := p.CleanState(); errDel != nil {
				log.Errorf("VAppProcessor.Create().ClearError error: %v", errDel)
			}
		}
	}()

	// creates networks instances
	networks := make([]*types.OrgVDCNetwork, 0)
	networks = append(networks, p.vcdClient.Network.OrgVDCNetwork)

	// creates template vApp
	log.Info("VAppProcessor.Create().VCloudClient Creates new vApp and VM instead with single name %s", p.cfg.VAppName)

	// check if vApp by name already exists
	var vAppExists *govcd.VApp
	vAppExists, err = p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		if !errors.Is(err, govcd.ErrorEntityNotFound) {
			log.Errorf("VAppProcessor.Create().VCloudClient.GetVAppByName error: %v", err)
			return nil, err
		}
	}

	if vAppExists != nil {
		return nil, fmt.Errorf("vApp with a same name already exists: %s", p.cfg.VAppName)
	}

	// create a new vApp
	vApp, err := p.vcdClient.VirtualDataCenter.CreateRawVApp(p.cfg.VAppName, "Container Host created with Docker Host by VAppProcessor")
	if err != nil {
		log.Errorf("VAppProcessor.Create().VCloudClient.CreateRawVApp error: %v", err)
		return nil, err
	}

	taskNet, err := vApp.AddRAWNetworkConfig(networks)
	if err != nil {
		log.Errorf("VAppProcessor.Create.AddRAWNetworkConfig error: %v", err)
		return nil, err
	}

	err = taskNet.WaitTaskCompletion()
	if err != nil {
		log.Errorf("VAppProcessor.CreateRawVApp.WaitTaskCompletion p.vcdClient.virtualDataCenter.ComposeVApp error: %v", err)
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
		log.Errorf("VAppProcessor.AddNewVM.WaitTaskCompletion  p.vcdClient.virtualDataCenter.ComposeVApp error: %v", err)
		return nil, err
	}

	// get vApp by name to check if it was created correctly
	vApp, err = p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VAppProcessor.Create().VCloudClient.GetVAppByName error: %v", err)
		return nil, err
	}

	// get VM by name to check if it was created correctly
	virtualMachine, err := vApp.GetVMByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VAppProcessor.Create().VCloudClient.GetVMByName error: %v", err)
		return nil, err
	}

	log.Info("VAppProcessor.Create().VCloudClient Creates new vApp and VM instead with single name %s", p.cfg.VAppName)

	// Wait while VM is creating and powered off
	for {
		vApp, err = p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
		if err != nil {
			log.Errorf("VAppProcessor.Create.GetVAppByName error: with machine %d error: %v", p.cfg.VAppName, err)
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

			log.Infof("VAppProcessor.Create().VCloudClient waiting for vm %s created and powered off. Current status: %s", p.cfg.VAppName, status)

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
	if err != nil {
		log.Errorf("VAppProcessor.Create().vmPostSettings error: %v", err)
		return nil, err
	}

	// set custom configs if it's not empty
	if customCfg != nil {
		guestSection, errSection := prepareCustomSectionForVM(*virtualMachine.VM.GuestCustomizationSection, customCfg)
		if errSection != nil {
			return nil, fmt.Errorf("prepareCustomSectionForVM error: %w", errSection)
		}

		_, errSet := virtualMachine.SetGuestCustomizationSection(&guestSection)
		if errSet != nil {
			return nil, fmt.Errorf("SetGuestCustomizationSection error: %w", errSet)
		}
	}

	if p.cfg.EdgeGateway != "" && p.cfg.PublicIP != "" {
		if p.cfg.VdcEdgeGateway != "" {
			var vdcGateway *govcd.Vdc
			vdcGateway, err = p.vcdClient.Org.GetVDCByName(p.cfg.VdcEdgeGateway, true)
			if err != nil {
				log.Errorf("VAppProcessor.Create().GetVDCByName error: %v", err)

				return nil, err
			}

			var edge *govcd.EdgeGateway
			edge, err = vdcGateway.GetEdgeGatewayByName(p.cfg.EdgeGateway, true)
			if err != nil {
				log.Errorf("VAppProcessor.Create().GetEdgeGatewayByName error: %v", err)

				return nil, err
			}

			log.Infof("VAppProcessor Creating NAT and Firewall Rules on %s...", p.cfg.EdgeGateway)

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
				log.Errorf("VAppProcessor.Create.WaitTaskCompletion.WaitTaskCompletion error: %v", err)

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
func (p *VAppProcessor) vmPostSettings(vm *govcd.VM) error {
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

func (p *VAppProcessor) CleanState() error {
	log.Info("VAppProcessor.CleanState() running")

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VAppProcessor.CleanState().GetVAppByName error: %v", err)
		return err
	}

	if p.cfg.EdgeGateway != "" && p.cfg.PublicIP != "" {
		if p.cfg.VdcEdgeGateway != "" {
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

			log.Infof("VAppProcessor.Removing NAT and Firewall Rules on %s...", p.cfg.EdgeGateway)

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
				log.Errorf("VAppProcessor.Remove.Delete dnat error: %v", errDel)
				return err
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

	for {
		status, err := vApp.GetStatus()
		if err != nil {
			log.Errorf("VAppProcessor.Remove.GetStatus error: %v", err)
			return err
		}

		if status == "UNRESOLVED" {
			log.Infof("VAppProcessor.Remove.Unresolved waiting for %s...", p.cfg.VAppName)
			time.Sleep(1 * time.Second)
			continue
		}

		if status != "POWERED_OFF" {
			log.Infof("VAppProcessor.Remove machine :%s status is %s. Power it off", p.cfg.VAppName, status)
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
			log.Infof("VAppProcessor.Remove.Powered Off %s...", p.cfg.VAppName)
			break
		}
	}

	log.Infof("VAppProcessor.Remove.Delete %s...", p.cfg.VAppName)
	task, err := vApp.Delete()
	if err != nil {
		return err
	}

	if err = task.WaitTaskCompletion(); err != nil {
		log.Errorf("Remove.Undeploy.WaitTaskCompletion after undeploy error: %v", err)
		return err
	}

	log.Infof("Remove.Deleting %s...", p.cfg.VAppName)

	return nil
}

func prepareCustomSectionForVM(
	vmScript types.GuestCustomizationSection,
	customCfg interface{},
) (types.GuestCustomizationSection, error) {

	cfg, ok := customCfg.(*CustomScriptConfigVAppProcessor)
	if !ok {
		return types.GuestCustomizationSection{}, fmt.Errorf("invalid config type: %T", cfg)
	}

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

func (p *VAppProcessor) Remove() error {
	log.Infof("VAppProcessor.Remove %s...", p.cfg.VAppName)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VAppProcessor.Remove.GetVAppByName error: %v", err)
		return err
	}

	if p.cfg.EdgeGateway != "" && p.cfg.PublicIP != "" {
		if p.cfg.VdcEdgeGateway != "" {
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

			log.Infof("VAppProcessor.Removing NAT and Firewall Rules on %s...", p.cfg.EdgeGateway)

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

	status, err := vApp.GetStatus()
	if err != nil {
		log.Errorf("VAppProcessor.Remove.GetStatus error: %v", err)
		return err
	}

	if status == "POWERED_ON" {
		// If it's powered on, power it off before deleting
		log.Info("VAppProcessor.Remove() power it off %s...", p.cfg.VAppName)
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

	log.Debugf("VAppProcessor.Remove() Undeploying %s", p.cfg.VAppName)
	task, err := vApp.Undeploy()
	if err != nil {
		log.Errorf("VAppProcessor.Remove.Undeploy error: %v", err)
		return err
	}

	if err = task.WaitTaskCompletion(); err != nil {
		log.Errorf("VAppProcessor.Remove.WaitTaskCompletion error: %v", err)
		return err
	}

	log.Infof("VAppProcessor.Remove() Deleting %s", p.cfg.VAppName)

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
	log.Infof("VAppProcessor.Stop %s...", p.cfg.VAppName)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VAppProcessor.Stop.getVDCApp error: %v", err)
		return err
	}

	task, errTask := vApp.Shutdown()
	if errTask != nil {
		log.Errorf("VAppProcessor.Stop.PowerOff error: %v", errTask)
		return errTask
	}

	if errWait := task.WaitTaskCompletion(); errTask != nil {
		log.Errorf("VAppProcessor.Stop.WaitTaskCompletion error: %v", errWait)
		return errWait
	}

	return nil
}

func (p *VAppProcessor) Kill() error {
	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VAppProcessor.Kill.GetVAppByName error: %v", err)
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
	log.Info("VAppProcessor.Restart() running")

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VAppProcessor.Restart.GetVAppByName error: %v", err)
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
	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VAppProcessor.Start.GetVAppByName error: %v", err)
		return err
	}

	status, err := vApp.GetStatus()
	if err != nil {
		log.Errorf("VAppProcessor.Start.getVcdStatus.GetStatus error: %v", vApp)
		return err
	}

	log.Infof("VAppProcessor.Start.GetStatus current status :%s", status)

	if status == "POWERED_OFF" {
		log.Info("VAppProcessor.Start.VCloudClient Start machine %s", p.cfg.VAppName)
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
