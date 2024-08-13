package processor

import (
	"errors"
	"fmt"
	"github.com/cenkalti/backoff"
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

var mapVAppReadyStatuses = map[string]interface{}{
	"RESOLVED":    nil,
	"DEPLOYED":    nil,
	"POWERED_ON":  nil,
	"POWERED_OFF": nil,
	"MIXED":       nil,
}

// The execution status of the task. One of:
// queued (The task has been queued for execution)
// preRunning (The task is awaiting preprocessing or administrative action.)
// running (The task is running.)
// success (The task completed with a status of success.)
// error (The task encountered an error while running.)
// canceled (The task was canceled by the owner or an administrator.)
// aborted (The task was aborted by an administrative action.)
var mapTasksFinishedStatuses = map[string]interface{}{
	"success":  nil,
	"error":    nil,
	"canceled": nil,
	"aborted":  nil,
}

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
	RootAuth    bool
}

func NewVMProcessor(client *client.VCloudClient, cfg ConfigProcessor) Processor {
	return &VMProcessor{
		cfg:       cfg,
		vcdClient: client,
	}
}

func (p *VMProcessor) checkVAppExistsAndCreateIfNot() (*govcd.VApp, error) {
	log.Infof("VMProcessor.checkVAppExistsAndCreateIfNot running with config: %+v", p.cfg)

	vAppExist, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		if !errors.Is(err, govcd.ErrorEntityNotFound) {
			log.Errorf("VMProcessor.checkVAppExistsAndCreateIfNot.VCloudClient.GetVAppByName error: %v", err)
			return nil, err
		}
	}

	// if exists, do nothing
	if vAppExist != nil {
		return vAppExist, nil
	}

	log.Infof("VMProcessor.checkVAppExistsAndCreateIfNot VApp %s doesn't exist. Creates new vApp", p.cfg.VAppName)

	// creates networks instances
	networks := make([]*types.OrgVDCNetwork, 0)
	networks = append(networks, p.vcdClient.Network.OrgVDCNetwork)

	// create a new vApp
	vApp, err := p.vcdClient.VirtualDataCenter.CreateRawVApp(p.cfg.VAppName, "Container Host created with Docker Host by VMProcessor")
	if err != nil {
		log.Errorf("VMProcessor.checkVAppExistsAndCreateIfNot.CreateRawVApp error: %v", err)
		return nil, err
	}

	taskNet, err := vApp.AddRAWNetworkConfig(networks)
	if err != nil {
		log.Errorf("VMProcessor.checkVAppExistsAndCreateIfNot.AddRAWNetworkConfig error: %v", err)
		return nil, err
	}

	err = p.WaitReadyVAppAndRunTask(vApp, taskNet)
	if err != nil {
		log.Errorf("VMProcessor.checkVAppExistsAndCreateIfNot.WaitReadyVAppAndRunTask.AddRAWNetworkConfig error: %v", err)
		return nil, err
	}

	return vApp, nil
}

func (p *VMProcessor) Create(customCfg interface{}) (*govcd.VApp, error) {
	log.Infof("VMProcessor.Create running with config: %+v", p.cfg)

	var err error

	defer func() {
		if err != nil {
			log.Errorf("VMProcessor.cleanState reason ----> %v", err)
			if errDel := p.cleanState(); errDel != nil {
				log.Errorf("VMProcessor.cleanState error: %v", errDel)
			}
		}
	}()

	vApp, errVApp := p.checkVAppExistsAndCreateIfNot()
	if errVApp != nil {
		log.Errorf("VMProcessor.Create.checkVAppExistsAndCreateIfNot error: %v", errVApp)
		return nil, errVApp
	}

	// creates template vApp
	log.Infof("VMProcessor.Create creates new VM %s instead vApp %s", p.cfg.VMachineName, p.cfg.VAppName)

	// check if VM by name exists
	vmExists, errExists := vApp.GetVMByName(p.cfg.VMachineName, true)
	if errExists != nil {
		if !errors.Is(errExists, govcd.ErrorEntityNotFound) {
			log.Errorf("VMProcessor.Create.GetVMByName error: %v", errExists)
			err = errExists

			return nil, err
		}
	}

	if vmExists != nil {
		return nil, fmt.Errorf("VMProcessor.Create VM %s already exists in vApp: %s", p.cfg.VMachineName, p.cfg.VAppName)
	}

	// wait until vApp will be ready
	if err := p.endlessWaitAllVAppTasksBaclkoff(); err != nil {
		log.Errorf("VMProcessor.Create.endlessWaitVCDAppReadyStatusBackoff error: %v", err)
		return nil, err
	}

	log.Infof("VMProcessor.Create creates new VM #3 %s instead vApp %s", p.cfg.VMachineName, p.cfg.VAppName)

	if err := p.endlessWaitVCDAppReadyStatusBackoff(vApp); err != nil {
		log.Errorf("VMProcessor.Create.endlessWaitVCDAppReadyStatusBackoff error: %v", err)
		return nil, err
	}

	task, errVM := vApp.AddNewVM(
		p.cfg.VMachineName,
		p.vcdClient.VAppTemplate,
		p.vcdClient.VAppTemplate.VAppTemplate.Children.VM[0].NetworkConnectionSection,
		true,
	)
	if errVM != nil {
		log.Errorf("VMProcessor.Create.AddNewVM error => go to loop: %v", p.cfg.VMachineName)
		waitingFunc := func() error {
			task, errVM = vApp.AddNewVM(
				p.cfg.VMachineName,
				p.vcdClient.VAppTemplate,
				p.vcdClient.VAppTemplate.VAppTemplate.Children.VM[0].NetworkConnectionSection,
				true,
			)
			if errVM != nil {
				return fmt.Errorf("VMProcessor.Create.AddNewVM error => retry create VM: %v", p.cfg.VMachineName)
			}
			return nil
		}

		expBackoff := backoff.NewExponentialBackOff()
		expBackoff.InitialInterval = 1 * time.Second
		expBackoff.MaxInterval = 30 * time.Second

		// endless exponential backoff
		expBackoff.MaxElapsedTime = 0

		err := backoff.Retry(waitingFunc, expBackoff)
		if err != nil {
			log.Errorf("VMProcessor.Create.AddNewVM error => retry create VM with interval: %v", errVM)
			return nil, err
		}
	}

	if errTask := task.WaitTaskCompletion(); errTask != nil {
		log.Errorf("VMProcessor.Create.WaitTaskCompletion error")
		return nil, errTask
	}

	log.Infof("VMProcessor.Create creates new VM #4 %s instead vApp %s", p.cfg.VMachineName, p.cfg.VAppName)

	// get VM by name to check if it was created correctly
	virtualMachine, err := vApp.GetVMByName(p.cfg.VMachineName, true)
	if err != nil {
		log.Errorf("VMProcessor.Create.GetVMByName with VM %s error: %v", p.cfg.VMachineName, err)
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

			log.Infof("VMProcessor.Create waiting for VM %s created and powered off. Current status: %s", p.cfg.VAppName, status)

			if status == "POWERED_OFF" {
				virtualMachine = vm
				break
			}
		}

		time.Sleep(time.Second * 1)
	}

	// set post settings for VM
	log.Infof("VMProcessor.Create VM %s was created and powered off. Set post-settings before run VM", p.cfg.VMachineName)
	err = p.vmPostSettings(virtualMachine)
	if err != nil {
		log.Errorf("VMProcessor.Create.vmPostSettings error: %v", err)
		return nil, err
	}

	// set custom configs if it's not empty
	if customCfg != nil {
		var guestSection types.GuestCustomizationSection

		guestSection, err = p.prepareCustomSectionForVM(*virtualMachine.VM.GuestCustomizationSection, customCfg)
		if err != nil {
			return nil, fmt.Errorf("VMProcessor.Create.prepareCustomSectionForVM error: %w", err)
		}

		_, err = virtualMachine.SetGuestCustomizationSection(&guestSection)
		if err != nil {
			return nil, fmt.Errorf("VMProcessor.Create.SetGuestCustomizationSection error: %w", err)
		}
	}

	if p.cfg.EdgeGateway != "" && p.cfg.PublicIP != "" {
		if p.cfg.VdcEdgeGateway != "" {
			var vdcGateway *govcd.Vdc
			vdcGateway, err = p.vcdClient.Org.GetVDCByName(p.cfg.VdcEdgeGateway, true)
			if err != nil {
				log.Errorf("VMProcessor.Create.GetVDCByName error: %v", err)

				return nil, err
			}

			var edge *govcd.EdgeGateway
			edge, err = vdcGateway.GetEdgeGatewayByName(p.cfg.EdgeGateway, true)
			if err != nil {
				log.Errorf("VMProcessor.Create.GetEdgeGatewayByName error: %v", err)

				return nil, err
			}

			log.Infof("VMProcessor.Create Creating NAT and Firewall Rules on %s...", p.cfg.EdgeGateway)

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

			if err := task1To1Map.WaitTaskCompletion(); err != nil {
				log.Errorf("VMProcessor.Create.WaitReadyVAppAndRunTask.Create1to1Mapping error: %v", err)
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

	// Get status of VM and Power it ON if VM has different status
	status, errStatus := virtualMachine.GetStatus()
	if errStatus != nil {
		log.Errorf("VMProcessor.Create.GetStatus error: %v", errStatus)
		return nil, errStatus
	}

	if status != "POWERED_ON" {
		task, err := virtualMachine.PowerOn()
		if err != nil {
			log.Errorf("VMProcessor.Create.GetStatus PowerOn: %v", err)
			return nil, err
		}

		if err := task.WaitTaskCompletion(); err != nil {
			log.Errorf("VMProcessor.TaskWithReadyVApp.WaitTaskCompletion error: %v", err)
			return nil, err
		}
	}

	return vApp, nil
}

func (p *VMProcessor) Remove() error {
	log.Infof("VMProcessor.Remove running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppById(p.cfg.VAppID, true)
	if err != nil {
		log.Errorf("VMProcessor.Remove.GetVAppById error: %v", err)
		return err
	}

	virtualMachine, err := vApp.GetVMById(p.cfg.VMachineID, true)
	if err != nil {
		log.Errorf("VMProcessor.Remove.GetVMById error: %v", err)
		return err
	}

	// If it's powered on, power it off before deleting
	status, errStatus := virtualMachine.GetStatus()
	if errStatus != nil {
		log.Errorf("VMProcessor.Remove.GetStatus error: %v", errStatus)
		return errStatus
	}

	if status != "POWERED_OFF" {
		// If it's powered on, power it off before deleting
		log.Infof("VMProcessor.Remove VM with name %s in vApp name %s", p.cfg.VMachineName, p.cfg.VAppName)

		task, errTask := virtualMachine.PowerOff()
		if errTask != nil {
			log.Errorf("VMProcessor.Remove.PowerOff error: %v", errTask)
			return errTask
		}

		if err := p.WaitReadyVAppAndRunTask(vApp, task); err != nil {
			log.Errorf("VMProcessor.Remove.WaitReadyVAppAndRunTask.VM.PowerOff error: %v", err)
			return err
		}
	}

	// unmount disks
	disksSettings := virtualMachine.VM.VmSpecSection.DiskSection.DiskSettings
	for _, diskSpec := range disksSettings {
		if diskSpec.UnitNumber == 0 {
			log.Infof("VMProcessor.Remove.DeleteInternalDisk ignore OS disk with id %s", diskSpec.DiskId)
			continue
		}
		log.Infof("VMProcessor.Remove.DeleteInternalDisk with id %s, name: %s", diskSpec.DiskId, diskSpec.Disk.Name)
		task, errTask := virtualMachine.DetachDisk(&types.DiskAttachOrDetachParams{
			Disk: &types.Reference{
				HREF: diskSpec.Disk.HREF,
			},
		})
		if errTask != nil {
			log.Errorf("VMProcessor.Remove.DeleteInternalDisk error: %v", errTask)
			return errTask
		}

		if err = task.WaitTaskCompletion(); err != nil {
			log.Errorf("VMProcessor.Remove.DeleteInternalDisk error: %v", err)
			return err
		}
	}

	log.Infof("VMProcessor.Remove.DeleteAsync deleting VM %s in app: %s", p.cfg.VMachineName, p.cfg.VAppName)

	task, err := virtualMachine.DeleteAsync()
	if err != nil {
		log.Errorf("VMProcessor.Remove.DeleteAsync error: %v", err)
		return err
	}

	if err := p.WaitReadyVAppAndRunTask(vApp, task); err != nil {
		log.Errorf("VMProcessor.Remove.WaitReadyVAppAndRunTask.VM.DeleteAsync error: %v", err)
		return err
	}

	return nil
}

func (p *VMProcessor) Stop() error {
	log.Infof("VMProcessor.Stop running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppById(p.cfg.VAppID, true)
	if err != nil {
		log.Errorf("VMProcessor.Stop.GetVAppById error: %v", err)
		return err
	}

	virtualMachine, err := vApp.GetVMById(p.cfg.VMachineID, true)
	if err != nil {
		log.Errorf("VMProcessor.Stop.GetVMById error: %v", err)
		return err
	}

	task, err := virtualMachine.PowerOff()
	if err != nil {
		log.Errorf("VMProcessor.Stop.PowerOff error: %v", err)
		return err
	}

	if err := p.WaitReadyVAppAndRunTask(vApp, task); err != nil {
		log.Errorf("VMProcessor.Stop.WaitReadyVAppAndRunTask.VM.PowerOff error: %v", err)
		return err
	}

	return nil
}

func (p *VMProcessor) Kill() error {
	log.Infof("VMProcessor.Kill running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppById(p.cfg.VAppID, true)
	if err != nil {
		log.Errorf("VMProcessor.Kill.GetVAppById error: %v", err)
		return err
	}

	virtualMachine, err := vApp.GetVMById(p.cfg.VMachineID, true)
	if err != nil {
		log.Errorf("VMProcessor.Kill.GetVMById error: %v", err)
		return err
	}

	task, err := virtualMachine.PowerOff()
	if err != nil {
		log.Errorf("VMProcessor.Kill.PowerOff error: %v", err)
		return err
	}

	if err := p.WaitReadyVAppAndRunTask(vApp, task); err != nil {
		log.Errorf("VMProcessor.Stop.WaitReadyVAppAndRunTask.VM.PowerOff error: %v", err)
		return err
	}

	// unmount disks
	disksSettings := virtualMachine.VM.VmSpecSection.DiskSection.DiskSettings
	for _, diskSpec := range disksSettings {
		if diskSpec.UnitNumber == 0 {
			log.Infof("VMProcessor.Remove.DeleteInternalDisk ignore OS disk with id %s", diskSpec.DiskId)
			continue
		}
		log.Infof("VMProcessor.Remove.DeleteInternalDisk with id %s, name: %s", diskSpec.DiskId, diskSpec.Disk.Name)
		task, errTask := virtualMachine.DetachDisk(&types.DiskAttachOrDetachParams{
			Disk: &types.Reference{
				HREF: diskSpec.Disk.HREF,
			},
		})
		if errTask != nil {
			log.Errorf("VMProcessor.Remove.DeleteInternalDisk error: %v", errTask)
			return errTask
		}

		if err = task.WaitTaskCompletion(); err != nil {
			log.Errorf("VMProcessor.Remove.DeleteInternalDisk error: %v", err)
			return err
		}
	}

	err = virtualMachine.Delete()
	if err != nil {
		log.Errorf("VMProcessor.Kill.Delete error: %v", err)
		return err
	}

	return nil
}

func (p *VMProcessor) Restart() error {
	log.Infof("VMProcessor.Restart running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppById(p.cfg.VAppID, true)
	if err != nil {
		log.Errorf("VMProcessor.Restart.GetVAppById error: %v", err)
		return err
	}

	virtualMachine, err := vApp.GetVMById(p.cfg.VMachineID, true)
	if err != nil {
		log.Errorf("VMProcessor.Restart.GetVMById error: %v", err)
		return err
	}

	task, err := virtualMachine.PowerOff()
	if err != nil {
		log.Errorf("VMProcessor.Restart.PowerOff error: %v", err)
		return err
	}

	if err := p.WaitReadyVAppAndRunTask(vApp, task); err != nil {
		log.Errorf("VMProcessor.Restart.WaitReadyVAppAndRunTask.VM.PowerOff error: %v", err)
		return err
	}

	// wait while vm powered off
	for {
		vm, errName := vApp.GetVMById(p.cfg.VMachineID, true)
		if errName != nil {
			log.Errorf("VMProcessor.Restart.GetVMById error: %v", errName)
			return errName
		}

		status, err := vm.GetStatus()
		if err != nil {
			log.Errorf("VMProcessor.Restart.GetStatus error: %v", err)
			return err
		}

		log.Infof("VMProcessor.Restart VM : %s current status :%s", p.cfg.VMachineName, status)

		if status == "POWERED_OFF" {
			virtualMachine = vm
			break
		} else {
			time.Sleep(2 * time.Second)
		}
	}

	task, err = virtualMachine.PowerOn()
	if err != nil {
		log.Errorf("VMProcessor.Restart.PowerOn error: %v", err)
		return err
	}

	if err := p.WaitReadyVAppAndRunTask(vApp, task); err != nil {
		log.Errorf("VMProcessor.Restart.WaitReadyVAppAndRunTask.VM.PowerOn error: %v", err)
		return err
	}

	return nil
}

func (p *VMProcessor) Start() error {
	log.Infof("VMProcessor.Start running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppById(p.cfg.VAppID, true)
	if err != nil {
		log.Errorf("VMProcessor.Start.GetVAppById error: %v", err)
		return err
	}

	virtualMachine, err := vApp.GetVMById(p.cfg.VMachineID, true)
	if err != nil {
		log.Errorf("VMProcessor.Start.GetVMById error: %v", err)
		return err
	}

	status, err := virtualMachine.GetStatus()
	if err != nil {
		log.Errorf("VMProcessor.Start.GetStatus error: %v", vApp)
		return err
	}

	log.Infof("VMProcessor.Start current status :%s", status)

	if status == "POWERED_OFF" {
		log.Infof("VMProcessor.Start run machine %s with id : %s in vapp %s", p.cfg.VMachineName, p.cfg.VMachineID, p.cfg.VAppName)

		task, errOn := virtualMachine.PowerOn()
		if errOn != nil {
			log.Errorf("VMProcessor.Start.PowerOn error: %v", errOn)
			return errOn
		}

		if err := p.WaitReadyVAppAndRunTask(vApp, task); err != nil {
			log.Errorf("VMProcessor.Restart.WaitReadyVAppAndRunTask.VM.PowerOn error: %v", err)
			return err
		}
	}

	return nil
}

func (p *VMProcessor) vmPostSettings(vm *govcd.VM) error {
	log.Infof("VMProcessor.vmPostSettings running with custom config: %+v", p.cfg)

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
		return fmt.Errorf("VMProcessor.vmPostSettings.UpdateVmSpecSection error: %w", err)
	}

	return nil
}

func (p *VMProcessor) GetState() (state.State, error) {
	log.Infof("VMProcessor.GetState running with config: %+v", p.cfg)

	vApp, errApp := p.vcdClient.VirtualDataCenter.GetVAppById(p.cfg.VAppID, true)
	if errApp != nil {
		log.Errorf("VMProcessor.GetState.GetVAppById error: %v", errApp)
		return state.None, errApp
	}

	vm, err := vApp.GetVMById(p.cfg.VMachineID, true)
	if err != nil {
		log.Errorf("VMProcessor.GetState.GetVMById error: %v", err)
		return state.None, err
	}

	status, errStatus := vm.GetStatus()
	if errStatus != nil {
		log.Errorf("VMProcessor.GetState.GetStatus error: %v", errStatus)
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
		return types.GuestCustomizationSection{}, fmt.Errorf("VMProcessor.prepareCustomSectionForVM invalid config type: %T", cfg)
	}

	log.Infof("VMProcessor.prepareCustomSectionForVM running with custom config: %+v", cfg)

	var (
		section  types.GuestCustomizationSection
		scriptSh string
	)

	section = vmScript

	section.ComputerName = cfg.MachineName

	section.AdminPasswordEnabled = &cfg.RootAuth

	scriptSh = cfg.InitData + "\n"
	// append ssh user to script
	scriptSh += "\nuseradd -m -d /home/" + cfg.SSHUser + " -s /bin/bash " + cfg.SSHUser + "\nmkdir -p /home/" + cfg.SSHUser + "/.ssh\nchmod 700 /home/" + cfg.SSHUser + "/.ssh\ntouch /home/" + cfg.SSHUser + "/.ssh/authorized_keys\nchmod 600 /home/" + cfg.SSHUser + "/.ssh/authorized_keys\necho \"" + strings.TrimSpace(cfg.SSHKey) + "\" > /home/" + cfg.SSHUser + "/.ssh/authorized_keys\necho \"" + cfg.SSHUser + "     ALL=(ALL) NOPASSWD:ALL\" >>  /etc/sudoers\nchown -R " + cfg.SSHUser + ". -R /home/" + cfg.SSHUser + "\n"

	if cfg.Rke2 {
		// if rke2
		readUserData, errRead := os.ReadFile(cfg.UserData)
		if errRead != nil {
			log.Errorf("VMProcessor.prepareCustomSection.ReadFile error: %s", errRead)
			return types.GuestCustomizationSection{}, errRead
		}

		cloudInit := rancher.GetCloudInitRancher(string(readUserData))

		log.Infof("VMProcessor.prepareCustomSection ----> rke2: %v Generate /usr/local/custom_script/install.sh file", cfg.Rke2)

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

func (p *VMProcessor) cleanState() error {
	log.Infof("VMProcessor.cleanState running with config: %+v", p.cfg)

	vApp, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
	if err != nil {
		log.Errorf("VMProcessor.cleanState.GetVAppByName error: %v", err)
		return err
	}

	virtualMachine, err := vApp.GetVMByName(p.cfg.VMachineName, true)
	if err != nil {
		log.Errorf("VMProcessor.cleanState().GetVMByName error: %v", err)
		return err
	}

	for {
		status, err := virtualMachine.GetStatus()
		if err != nil {
			log.Errorf("VMProcessor.cleanState.GetStatus error: %v", err)
			return err
		}

		if status == "UNRESOLVED" {
			log.Infof("VMProcessor.cleanState.Unresolved waiting for %s...", p.cfg.VAppName)
			time.Sleep(1 * time.Second)
			continue
		}

		if status != "POWERED_OFF" {
			log.Infof("VMProcessor.cleanState machine :%s status is %s. Power it off", p.cfg.VAppName, status)

			task, err := virtualMachine.PowerOff()
			if err != nil {
				log.Errorf("VMProcessor.cleanState.PowerOff error: %v", err)
				return err
			}

			if err := p.WaitReadyVAppAndRunTask(vApp, task); err != nil {
				log.Errorf("VMProcessor.cleanState.WaitReadyVAppAndRunTask.VM.PowerOff error: %v", err)
				return err
			}

			break
		} else {
			log.Infof("VMProcessor.cleanState wait %s...", p.cfg.VMachineName)
			break
		}
	}

	task, err := virtualMachine.DeleteAsync()
	if err != nil {
		log.Errorf("VMProcessor.DeleteAsync error: %v", err)
		return err
	}

	if err := p.WaitReadyVAppAndRunTask(vApp, task); err != nil {
		log.Errorf("VMProcessor.cleanState.WaitReadyVAppAndRunTask.VM.DeleteAsync error: %v", err)
		return err
	}

	log.Infof("VMProcessor.cleanState %s...", p.cfg.VMachineName)

	return nil
}

// endlessWaitAllVAppTasksBaclkoff - endless waiting for correct vApp status
func (p *VMProcessor) endlessWaitAllVAppTasksBaclkoff() error {
	waitingFunc := func() error {
		// get actual vApp state
		vApp, err := p.vcdClient.VirtualDataCenter.GetVAppByName(p.cfg.VAppName, true)
		if err != nil {
			log.Errorf("VMProcessor.endlessWaitVAppReadyStatus.GetVAppByName error: %v", err)
		}
		if vApp.VApp.Tasks == nil {
			return nil
		}
		// wait until ALL tasks will be finished
		for _, task := range vApp.VApp.Tasks.Task {
			_, ok := mapTasksFinishedStatuses[task.Status]
			if !ok {
				log.Infof("VMProcessor.endlessWaitVAppReadyStatus one of tasks have status: %s", task.Status)
				return fmt.Errorf("VMProcessor.endlessWaitVAppReadyStatus one of tasks have status: %s", task.Status)
			}
		}

		return nil
	}

	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 1 * time.Second
	expBackoff.MaxInterval = 30 * time.Second

	// endless exponential backoff
	expBackoff.MaxElapsedTime = 0

	err := backoff.Retry(waitingFunc, expBackoff)
	if err != nil {
		log.Errorf("VMProcessor.waitVappReadyBackoffAndDo error: %v", err)
		return err
	}

	return nil
}

// endlessWaitVCDAppReadyStatusBackoff - endless waiting for correct vApp status
func (p *VMProcessor) endlessWaitVCDAppReadyStatusBackoff(vApp *govcd.VApp) error {
	waitingFunc := func() error {
		status, err := vApp.GetStatus()
		if err != nil {
			log.Errorf("VMProcessor.endlessWaitVAppReadyStatus.GetStatus error: %v", err)
			return err
		}

		log.Infof("VMProcessor.endlessWaitVAppReadyStatus.GetStatus current status: %s", status)

		// found ready status
		_, ok := mapVAppReadyStatuses[status]
		if ok {
			return nil
		}

		return fmt.Errorf("VMProcessor.endlessWaitVAppReadyStatus.GetStatus retry status: %s", status)
	}

	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 1 * time.Second
	expBackoff.MaxInterval = 30 * time.Second

	// endless exponential backoff
	expBackoff.MaxElapsedTime = 0

	err := backoff.Retry(waitingFunc, expBackoff)
	if err != nil {
		log.Errorf("VMProcessor.waitVappReadyBackoffAndDo error: %v", err)
		return err
	}

	return nil
}

// WaitReadyVAppAndRunTask - wait until vApp will be ready and run task
func (p *VMProcessor) WaitReadyVAppAndRunTask(vApp *govcd.VApp, task govcd.Task) error {
	// wait until vApp will be ready
	if err := p.endlessWaitVCDAppReadyStatusBackoff(vApp); err != nil {
		log.Errorf("VMProcessor.TaskWithReadyVApp.endlessWaitVCDAppReadyStatusBackoff before task error: %v", err)
		return err
	}

	if err := task.WaitTaskCompletion(); err != nil {
		log.Errorf("VMProcessor.TaskWithReadyVApp.WaitTaskCompletion error: %v", err)
		return err
	}

	// wait until vApp will be ready after task
	if err := p.endlessWaitVCDAppReadyStatusBackoff(vApp); err != nil {
		log.Errorf("VMProcessor.TaskWithReadyVApp.endlessWaitVCDAppReadyStatusBackoff after task error: %v", err)
		return err
	}

	return nil
}
