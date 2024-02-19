package vmwarevcloud

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/docker/machine/libmachine/drivers"
	"github.com/docker/machine/libmachine/log"
	"github.com/docker/machine/libmachine/mcnflag"
	"github.com/docker/machine/libmachine/ssh"
	"github.com/docker/machine/libmachine/state"
	"github.com/vmware/go-vcloud-director/v2/govcd"
	"github.com/vmware/go-vcloud-director/v2/types/v56"
	"gopkg.in/yaml.v2"
)

const (
	ubuntu20 = "ubuntu20"
	ubuntu18 = "ubuntu18"
)

type Driver struct {
	*drivers.BaseDriver
	// machine config
	Rke2           bool
	MachineAppID   string
	MachineName    string
	DockerPort     int
	PrivateIP      string
	PublicIP       string
	EdgeGateway    string
	VdcEdgeGateway string
	NumCPUs        int
	MemorySizeMB   int
	DiskSizeMB     int

	// SSH config
	InitData string
	UserData string
	SSHPort  int

	// VCD config
	AdapterType             string
	IPAddressAllocationMode string
	Href                    string
	UserName                string
	UserPassword            string
	Organization            string
	VDC                     string
	OrgVDCNet               string
	Catalog                 string
	CatalogItem             string
	StoreProfile            string
	VAppID                  string
	URL                     *url.URL
	Insecure                bool
}

type RancherCloudInit struct {
	Runcmd     []string `yaml:"runcmd"`
	WriteFiles []struct {
		Content     string `yaml:"content"`
		Encoding    string `yaml:"encoding"`
		Path        string `yaml:"path"`
		Permissions string `yaml:"permissions"`
	} `yaml:"write_files"`
}

func NewDriver(hostName, storePath string) drivers.Driver {
	return &Driver{
		Catalog:                 defaultCatalog,
		CatalogItem:             defaultCatalogItem,
		NumCPUs:                 defaultCpus,
		MemorySizeMB:            defaultMemory,
		DiskSizeMB:              defaultDisk,
		DockerPort:              defaultDockerPort,
		Insecure:                defaultInsecure,
		Rke2:                    defaultRke2,
		AdapterType:             defaultAdapterType,
		IPAddressAllocationMode: defaultIPAddressAllocationMode,
		BaseDriver: &drivers.BaseDriver{
			SSHPort:     defaultSSHPort,
			MachineName: hostName,
			StorePath:   storePath,
		},
	}
}

// GetCreateFlags registers the flags this driver adds to
// "docker hosts create"
func (d *Driver) GetCreateFlags() []mcnflag.Flag {
	return []mcnflag.Flag{
		mcnflag.StringFlag{
			EnvVar: "VCD_USERNAME",
			Name:   "vcd-username",
			Usage:  "vCloud Director username",
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_PASSWORD",
			Name:   "vcd-password",
			Usage:  "vCloud Director password",
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_VDC",
			Name:   "vcd-vdc",
			Usage:  "vCloud Director Virtual Data Center",
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_VDCEDGEGATEWAY",
			Name:   "vcd-vdcedgegateway",
			Usage:  "vCloud Director Virtual Data Center Edge Gateway",
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_ORG",
			Name:   "vcd-org",
			Usage:  "vCloud Director Organization",
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_ORGVDCNETWORK",
			Name:   "vcd-orgvdcnetwork",
			Usage:  "vCloud Director Org VDC Network",
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_NETWORKADAPTERTYPE",
			Name:   "vcd-networkadaptertype",
			Usage:  "vCloud Director Network Adapter Type like VMXNET3",
			Value:  "",
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_IPADDRESSALLOCATIONMODE",
			Name:   "vcd-ipaddressallocationmode",
			Usage:  "vCloud Director IP Address Allocation Mode like DHCP",
			Value:  defaultIPAddressAllocationMode,
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_EDGEGATEWAY",
			Name:   "vcd-edgegateway",
			Usage:  "vCloud Director Edge Gateway (Default is <vdc>)",
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_PUBLICIP",
			Name:   "vcd-publicip",
			Usage:  "vCloud Director Org Public IP to use",
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_CATALOG",
			Name:   "vcd-catalog",
			Usage:  "vCloud Director Catalog (default is Public Catalog)",
			Value:  defaultCatalog,
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_CATALOGITEM",
			Name:   "vcd-catalogitem",
			Usage:  "vCloud Director Catalog Item (default is Ubuntu Precise)",
			Value:  defaultCatalogItem,
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_STORPROFILE",
			Name:   "vcd-storprofile",
			Usage:  "vCloud Storage Profile name",
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_HREF",
			Name:   "vcd-href",
			Usage:  "vCloud Director API Endpoint",
		},
		mcnflag.BoolFlag{
			EnvVar: "VCD_INSECURE",
			Name:   "vcd-insecure",
			Usage:  "vCloud Director allow non secure connections",
		},
		mcnflag.BoolFlag{
			EnvVar: "VCD_RKE2",
			Name:   "vcd-rke2",
			Usage:  "Allows user rancher RKE2 provisioning fix custom-install-script",
		},
		mcnflag.IntFlag{
			EnvVar: "VCD_CPU_COUNT",
			Name:   "vcd-cpu-count",
			Usage:  "vCloud Director VM Cpu Count (default 1)",
			Value:  defaultCpus,
		},
		mcnflag.IntFlag{
			EnvVar: "VCD_MEMORY_SIZE",
			Name:   "vcd-memory-size",
			Usage:  "vCloud Director VM Memory Size in MB (default 2048)",
			Value:  defaultMemory,
		},
		mcnflag.IntFlag{
			EnvVar: "VCD_DISK_SIZE",
			Name:   "vcd-disk-size",
			Usage:  "vCloud Director VM Disk Size in MB (default 20480)",
			Value:  defaultDisk,
		},
		mcnflag.IntFlag{
			EnvVar: "VCD_SSH_PORT",
			Name:   "vcd-ssh-port",
			Usage:  "vCloud Director SSH port",
			Value:  defaultSSHPort,
		},
		mcnflag.IntFlag{
			EnvVar: "VCD_DOCKER_PORT",
			Name:   "vcd-docker-port",
			Usage:  "vCloud Director Docker port",
			Value:  defaultDockerPort,
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_SSH_USER",
			Name:   "vcd-ssh-user",
			Usage:  "vCloud Director SSH user",
			Value:  defaultSSHUser,
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_USER_DATA",
			Name:   "vcd-user-data",
			Usage:  "Cloud-init based User data",
			Value:  "",
		},
		mcnflag.StringFlag{
			EnvVar: "VCD_INIT_DATA",
			Name:   "vcd-init-data",
			Usage:  "Cloud-init based User data before everything",
			Value:  "",
		},
	}
}

func (d *Driver) SetConfigFromFlags(flags drivers.DriverOptions) error {
	d.UserName = flags.String("vcd-username")
	d.UserPassword = flags.String("vcd-password")
	d.VDC = flags.String("vcd-vdc")
	d.Organization = flags.String("vcd-org")
	d.Href = flags.String("vcd-href")
	d.Insecure = flags.Bool("vcd-insecure")
	d.Rke2 = flags.Bool("vcd-rke2")
	d.PublicIP = flags.String("vcd-publicip")
	d.StoreProfile = flags.String("vcd-storprofile")
	d.UserData = flags.String("vcd-user-data")
	d.InitData = flags.String("vcd-init-data")
	d.AdapterType = flags.String("vcd-networkadaptertype")
	d.IPAddressAllocationMode = flags.String("vcd-ipaddressallocationmode")
	d.SetSwarmConfigFromFlags(flags)

	// Check for required Params
	if d.UserName == "" || d.UserPassword == "" || d.Href == "" || d.VDC == "" || d.Organization == "" || d.StoreProfile == "" {
		return fmt.Errorf("please specify vclouddirector mandatory params using options: -vcd-username -vcd-password -vcd-vdc -vcd-href -vcd-org and -vcd-storprofile")
	}

	u, err := url.ParseRequestURI(d.Href)
	if err != nil {
		return fmt.Errorf("Unable to pass url: %s", err)
	}
	d.URL = u

	// If the Org VDC Network is empty, set it to the default routed network.
	if flags.String("vcd-orgvdcnetwork") == "" {
		d.OrgVDCNet = flags.String("vcd-vdc") + "-default-routed"
	} else {
		d.OrgVDCNet = flags.String("vcd-orgvdcnetwork")
	}

	d.EdgeGateway = flags.String("vcd-edgegateway")
	d.VdcEdgeGateway = flags.String("vcd-vdcedgegateway")

	d.Catalog = flags.String("vcd-catalog")
	d.CatalogItem = flags.String("vcd-catalogitem")

	d.DockerPort = flags.Int("vcd-docker-port")
	d.SSHUser = flags.String("vcd-ssh-user")
	d.SSHPort = flags.Int("vcd-ssh-port")
	d.NumCPUs = flags.Int("vcd-cpu-count")
	d.MemorySizeMB = flags.Int("vcd-memory-size")
	d.DiskSizeMB = flags.Int("vcd-disk-size")
	d.PrivateIP = d.PublicIP

	return nil
}

func (d *Driver) GetURL() (string, error) {
	if err := drivers.MustBeRunning(d); err != nil {
		return "", err
	}

	return fmt.Sprintf("tcp://%s", net.JoinHostPort(d.PrivateIP, strconv.Itoa(d.DockerPort))), nil
}

func (d *Driver) GetIP() (string, error) {
	return d.PrivateIP, nil
}

func (d *Driver) GetSSHHostname() (string, error) {
	return d.GetIP()
}

// DriverName returns the name of the driver
func (d *Driver) DriverName() string {
	return "vcd"
}

func (d *Driver) GetState() (state.State, error) {
	log.Info("GetState() running")

	// check vcd platform state
	vdcClient := NewVCloudClient(d)

	vApp, errVdc := vdcClient.getVDCApp(d)
	if errVdc != nil {
		log.Errorf("Start.getVDC error: %v", errVdc)
		return state.Error, errVdc
	}

	status, errStatus := vApp.GetStatus()
	if errStatus != nil {
		log.Errorf("Start.getVcdStatus.GetStatus error: %v", errStatus)
		return state.Error, errStatus
	}

	switch status {
	case "POWERED_ON":
		return state.Running, nil
	case "POWERED_OFF":
		return state.Stopped, nil
	default:
		return state.None, nil
	}
}

func (d *Driver) Create() error {
	log.Info("Create() running")

	var errDel error

	defer func() {
		if errDel != nil {
			if err := d.deleteMachineError(errDel); err != nil {
				log.Errorf("Create.deleteMachineError error: %v", err)
			}
		}
	}()

	vdcClient := NewVCloudClient(d)

	errBuild := vdcClient.buildInstance(d)
	if errBuild != nil {
		log.Errorf("Create.buildInstance vdc error: %v", errBuild)
		return errBuild
	}

	log.Info("Create.VCloudClient Set up VApp before running")

	// change VApp template
	//if d.AdapterType != "" {
	//	vdcClient.vAppTemplate.VAppTemplate.NetworkConnectionSection = &types.NetworkConnectionSection{}
	//
	//	vdcClient.vAppTemplate.VAppTemplate.NetworkConnectionSection.NetworkConnection = append(
	//		vdcClient.vAppTemplate.VAppTemplate.NetworkConnectionSection.NetworkConnection,
	//		&types.NetworkConnection{
	//			Network:                 d.OrgVDCNet,
	//			NetworkAdapterType:      d.AdapterType,
	//			IPAddressAllocationMode: d.IPAddressAllocationMode,
	//			NetworkConnectionIndex:  0,
	//			IsConnected:             true,
	//			NeedsCustomization:      false,
	//		},
	//	)
	//}

	networks := make([]*types.OrgVDCNetwork, 0)
	networks = append(networks, vdcClient.network.OrgVDCNetwork)

	log.Info("Create.VCloudClient Creates new vApp and virtual machine")

	// Up vApp with template
	task, errCompose := vdcClient.virtualDataCenter.ComposeVApp(
		networks,
		vdcClient.vAppTemplate,
		vdcClient.storageProfileRef,
		d.MachineName,
		"Container Host created with Docker Host",
		true,
	)
	if errCompose != nil {
		log.Errorf("Create.ComposeVApp error: %v", errCompose)
		return errCompose
	}

	// Wait for the creation to be completed
	if errTask := task.WaitTaskCompletion(); errTask != nil {
		log.Errorf("Create.WaitTaskCompletion  vdcClient.virtualDataCenter.ComposeVApp error: %v", errTask)
		return errTask
	}

	vApp, errApp := vdcClient.virtualDataCenter.GetVAppByName(d.MachineName, true)
	if errApp != nil {
		log.Errorf("Create.GetVAppByName error: with machine %d error: %v", d.MachineName, errApp)
		return errApp
	}

	if d.AdapterType != "" {
		log.Infof("Create.Change VApp template to adapter type %s", d.AdapterType)
		netCfg, err := vApp.GetNetworkConnectionSection()
		if err != nil {
			log.Errorf("Create.GetNetworkConnectionSection error: %v", err)
			return err
		}

		netCfg.NetworkConnection = []*types.NetworkConnection{
			{
				Network:                 d.OrgVDCNet,
				NetworkAdapterType:      d.AdapterType,
				IPAddressAllocationMode: d.IPAddressAllocationMode,
				NetworkConnectionIndex:  0,
				IsConnected:             true,
				NeedsCustomization:      false,
			},
		}
	}

	virtualMachine, errMachine := vApp.GetVMByName(d.MachineName, true)
	if errMachine != nil {
		log.Errorf("Create.GetVMByName error: %v", errMachine)
		return errMachine
	}

	log.Info("Create.wait waiting for vm")

	// Wait while vm is creating
	for {
		vApp, errVApp := vdcClient.virtualDataCenter.GetVAppByName(d.MachineName, true)
		if errVApp != nil {
			log.Errorf("Create.GetVAppByName error: with machine %d error: %v", d.MachineName, errVApp)
			return errVApp
		}

		vm, err := vApp.GetVMByName(d.MachineName, true)
		if err != nil {
			log.Errorf("Create.GetVMByName error: %v", err)
			return err
		}

		if vm.VM.VmSpecSection != nil {
			// when the VM will get its specs, check status of the VM
			status, errStatus := vApp.GetStatus()
			if errStatus != nil {
				log.Errorf("Create.GetStatus error: %v", errStatus)
				errDel = fmt.Errorf("GetStatus error %w", errStatus)

				return errStatus
			}

			log.Infof("Create.waiting for vm created and powered off. Current status: %s", status)

			if status == "POWERED_OFF" {
				virtualMachine = vm
				break
			}
		}

		time.Sleep(time.Second * 1)
	}

	log.Info("Create vm was created and powered off. Set post-settings before run VM")
	err := d.postSettingsVM(virtualMachine)
	if err != nil {
		log.Errorf("Create.postSettingsVM error: %v", err)
		errDel = fmt.Errorf("postSettingsVM error %w", err)

		return err
	}

	if d.EdgeGateway != "" && d.PublicIP != "" {
		if d.VdcEdgeGateway != "" {
			vdcGateway, err := vdcClient.org.GetVDCByName(d.VdcEdgeGateway, true)
			if err != nil {
				errDel = fmt.Errorf("GetVDCByName error %w", err)

				return err
			}

			edge, err := vdcGateway.GetEdgeGatewayByName(d.EdgeGateway, true)
			if err != nil {
				errDel = fmt.Errorf("GetEdgeGatewayByName error %w", err)

				return err
			}

			log.Infof("Creating NAT and Firewall Rules on %s...", d.EdgeGateway)

			task, errMap := edge.Create1to1Mapping(
				virtualMachine.VM.NetworkConnectionSection.NetworkConnection[0].IPAddress,
				d.PublicIP,
				d.MachineName,
			)
			if errMap != nil {
				log.Errorf("Create.Create1to1Mapping error: %v", errMap)
				errDel = fmt.Errorf("Create1to1Mapping error %w", errMap)

				return err
			}

			if errTask := task.WaitTaskCompletion(); errTask != nil {
				log.Errorf("Create.WaitTaskCompletion.WaitTaskCompletion error: %v", errMap)
				errDel = fmt.Errorf("WaitTaskCompletion error %w", errMap)

				return errTask
			}
		} else {
			snatRuleDefinition := &types.NsxtNatRule{
				Name:              d.MachineName + "_snat",
				Description:       d.MachineName,
				Enabled:           true,
				RuleType:          types.NsxtNatRuleTypeSnat,
				ExternalAddresses: virtualMachine.VM.NetworkConnectionSection.NetworkConnection[0].IPAddress,
				InternalAddresses: d.PublicIP,
				FirewallMatch:     types.NsxtNatRuleFirewallMatchBypass,
			}

			dnatRuleDefinition := &types.NsxtNatRule{
				Name:              d.MachineName + "_dnat",
				Description:       d.MachineName,
				Enabled:           true,
				RuleType:          types.NsxtNatRuleTypeDnat,
				ExternalAddresses: d.PublicIP,
				InternalAddresses: virtualMachine.VM.NetworkConnectionSection.NetworkConnection[0].IPAddress,
				FirewallMatch:     types.NsxtNatRuleFirewallMatchBypass,
			}

			adminOrg, errAdmin := vdcClient.client.GetAdminOrgByName(d.Organization)
			if errAdmin != nil {
				log.Errorf("Create.GetAdminOrgByName error: %v", errAdmin)
				errDel = fmt.Errorf("GetAdminOrgByName error %w", errAdmin)

				return errAdmin
			}

			edge, err := adminOrg.GetNsxtEdgeGatewayByName(d.EdgeGateway)
			if edge != nil {
				log.Errorf("Create.GetNsxtEdgeGatewayByName error: %v", err)
				errDel = fmt.Errorf("GetNsxtEdgeGatewayByName error %w", err)

				return err
			}

			_, err = edge.CreateNatRule(snatRuleDefinition)
			if err != nil {
				log.Errorf("Create.CreateNatRule error: %v", err)
				errDel = fmt.Errorf("CreateNatRule error %w", err)

				return err
			}

			_, err = edge.CreateNatRule(dnatRuleDefinition)
			if err != nil {
				log.Errorf("Create.CreateNatRule error: %v", err)
				errDel = fmt.Errorf("CreateNatRule error %w", err)

				return err
			}
		}
	}

	// try to run vApp and machine
	log.Infof("Create Try to run virtual machine %s", d.MachineName)
	task, errPowerOn := vApp.PowerOn()
	if errPowerOn != nil {
		log.Errorf("Create.PowerOn error: %v", errPowerOn)
		errDel = fmt.Errorf("CreateNatRule error %w", errPowerOn)

		return errPowerOn
	}

	if errTask := task.WaitTaskCompletion(); errTask != nil {
		log.Errorf("Create.PowerOn.WaitTaskCompletion error: %v", errTask)
		return errTask
	}

	// check status of VM after task powered on
	for {
		if virtualMachine.VM.NetworkConnectionSection.NetworkConnection[0].IPAddress != "" {
			d.PrivateIP = virtualMachine.VM.NetworkConnectionSection.NetworkConnection[0].IPAddress
			break
		}

		time.Sleep(2 * time.Second)
	}

	d.VAppID = vApp.VApp.ID
	ipAddress, errIP := d.GetIP()
	if errIP != nil {
		log.Errorf("Create.GetIP error: %v", errIP)
		return errIP
	}

	d.IPAddress = ipAddress

	return nil
}

func (d *Driver) Remove() error {
	log.Info("Remove() running")

	vdcClient := NewVCloudClient(d)

	errBuild := vdcClient.buildInstance(d)
	if errBuild != nil {
		log.Errorf("Remove.buildInstance vdc error: %v", errBuild)
		return errBuild
	}

	log.Info("Remove.VCloudClient Set up VApp before running")

	vApp, err := vdcClient.getVDCApp(d)
	if err != nil {
		log.Errorf("Remove.getVDCApp error: %v", err)
		return err
	}

	if d.EdgeGateway != "" && d.PublicIP != "" {
		if d.VdcEdgeGateway != "" {
			vdcGateway, err := vdcClient.org.GetVDCByName(d.VdcEdgeGateway, true)
			if err != nil {
				log.Errorf("Remove.GetVDCByName error: %v", err)
				return err
			}
			edge, err := vdcGateway.GetEdgeGatewayByName(d.EdgeGateway, true)
			if err != nil {
				log.Errorf("Remove.GetEdgeGatewayByName error: %v", err)
				return err
			}

			log.Infof("Removing NAT and Firewall Rules on %s...", d.EdgeGateway)

			task, err := edge.Remove1to1Mapping(vApp.VApp.Children.VM[0].NetworkConnectionSection.NetworkConnection[0].IPAddress, d.PublicIP)
			if err != nil {
				return err
			}
			if err = task.WaitTaskCompletion(); err != nil {
				return err
			}
		} else {
			adminOrg, err := vdcClient.client.GetAdminOrgByName(d.Organization)
			edge, err := adminOrg.GetNsxtEdgeGatewayByName(d.EdgeGateway)

			dnat, err := edge.GetNatRuleByName(d.MachineName + "_dnat")
			if err != nil {
				return err
			}

			if errDel := dnat.Delete(); errDel != nil {
				log.Errorf("Remove.Delete dnat error: %v", errDel)
				return err
			}

			snat, err := edge.GetNatRuleByName(d.MachineName + "_snat")
			if err != nil {
				return err
			}
			if errDel := snat.Delete(); errDel != nil {
				log.Errorf("Remove.Delete snat error: %v", errDel)
				return err
			}
		}
	}

	for {
		status, err := vApp.GetStatus()
		if err != nil {
			log.Errorf("Remove.GetStatus error: %v", err)
			return err
		}

		if status == "UNRESOLVED" {
			log.Infof("Remove.Unresolved waiting for %s...", d.MachineName)
			time.Sleep(1 * time.Second)
			continue
		}

		if status != "POWERED_OFF" {
			log.Infof("Remove machine :%s status is %s. Power it off", d.MachineName, status)
			task, err := vApp.PowerOff()

			if err != nil {
				log.Errorf("Remove.PowerOff error: %v", err)
				return err
			}

			if err = task.WaitTaskCompletion(); err != nil {
				log.Errorf("Remove.PowerOff.WaitTaskCompletion error: %v", err)
				return err
			}
			break
		} else {
			log.Infof("Remove.Powered Off %s...", d.MachineName)
			break
		}
	}

	log.Infof("Remove.Undeploying %s...", d.MachineName)
	task, err := vApp.Undeploy()
	if err != nil {
		return err
	}
	if err = task.WaitTaskCompletion(); err != nil {
		log.Errorf("Remove.Undeploy.WaitTaskCompletion after undeploy error: %v", err)
		return err
	}

	log.Infof("Remove.Deleting %s...", d.MachineName)

	task, err = vApp.Delete()
	if err != nil {
		log.Errorf("Remove.Delete error: %v", err)
		return err
	}
	if err = task.WaitTaskCompletion(); err != nil {
		log.Errorf("Remove.Delete.WaitTaskCompletion error: %v", err)
		return err
	}

	return nil
}

func (d *Driver) Start() error {
	log.Info("Start() running")

	// check vcd platform state
	vdcClient := NewVCloudClient(d)

	log.Info("Start.VCloudClient.getVDCApp")

	vApp, errVdc := vdcClient.getVDCApp(d)
	if errVdc != nil {
		log.Errorf("Start.getVDC error: %v", errVdc)
		return errVdc
	}

	status, err := vApp.GetStatus()
	if err != nil {
		log.Errorf("Start.getVcdStatus.GetStatus error: %v", vApp)
		return err
	}

	log.Infof("Start.GetStatus current status :%s", status)

	if status == "POWERED_OFF" {
		log.Info("Start.VCloudClient Start machine %s app id %d", d.MachineName, d.VAppID)
		task, errOn := vApp.PowerOn()
		if errOn != nil {
			log.Errorf("Start.PowerOn error: %v", errOn)
			return errOn
		}

		if errTask := task.WaitTaskCompletion(); errTask != nil {
			log.Errorf("Start.WaitTaskCompletion error: %v", errTask)
			return errTask
		}
	}

	d.IPAddress, err = d.GetIP()
	if err != nil {
		log.Errorf("Start.GetIP error: %v", err)
		return err
	}

	return nil
}

func (d *Driver) Stop() error {
	log.Info("Stop() running")

	vdcClient := NewVCloudClient(d)

	log.Info("Stop.VCloudClient.getVDCApp")

	vApp, errVdc := vdcClient.getVDCApp(d)
	if errVdc != nil {
		log.Errorf("Stop.getVDC error: %v", errVdc)
		return errVdc
	}

	task, errTask := vApp.Shutdown()
	if errTask != nil {
		log.Errorf("Stop.PowerOff error: %v", errTask)
		return errTask
	}

	if errWait := task.WaitTaskCompletion(); errTask != nil {
		log.Errorf("Stop.WaitTaskCompletion error: %v", errWait)
		return errWait
	}

	d.IPAddress = ""

	return nil
}

func (d *Driver) Restart() error {
	log.Info("Restart() running")

	vdcClient := NewVCloudClient(d)

	log.Info("Stop.VCloudClient.getVDCApp")

	vApp, errVdc := vdcClient.getVDCApp(d)
	if errVdc != nil {
		log.Errorf("Stop.getVDC error: %v", errVdc)
		return errVdc
	}

	task, err := vApp.Reset()
	if err != nil {
		log.Errorf("Stop.Reset error: %v", err)
		return err
	}

	if err = task.WaitTaskCompletion(); err != nil {
		log.Errorf("Stop.WaitTaskCompletion error: %v", err)
		return err
	}

	d.IPAddress, err = d.GetIP()
	return err
}

func (d *Driver) Kill() error {
	log.Info("Kill() running")

	vdcClient := NewVCloudClient(d)

	log.Info("Stop.VCloudClient.getVDCApp")

	vApp, errVdc := vdcClient.getVDCApp(d)
	if errVdc != nil {
		log.Errorf("Stop.getVDC error: %v", errVdc)
		return errVdc
	}

	task, errTask := vApp.PowerOff()
	if errTask != nil {
		log.Errorf("Stop.PowerOff error: %v", errTask)
		return errTask
	}

	if err := task.WaitTaskCompletion(); err != nil {
		log.Errorf("Stop.WaitTaskCompletion error: %v", err)
		return err
	}

	d.IPAddress = ""

	return nil
}

func (d *Driver) createSSHKey() (string, error) {
	if err := ssh.GenerateSSHKey(d.GetSSHKeyPath()); err != nil {
		log.Errorf("createSSHKey.GenerateSSHKey error: %s", err)
		return "", err
	}

	publicKey, err := os.ReadFile(d.GetSSHKeyPath() + ".pub")
	if err != nil {
		log.Errorf("createSSHKey.ReadFile error: %s", err)
		return "", err
	}

	return string(publicKey), nil
}

func getRancherCloudInit(s string) string {
	out := RancherCloudInit{}
	err := yaml.Unmarshal([]byte(s), &out)
	if err != nil {
		log.Debugf("Unmarshal: %v", err)
	}

	for _, entry := range out.WriteFiles {
		return entry.Content
	}

	return ""
}

func (d *Driver) postSettingsVM(vm *govcd.VM) error {
	var numCPUsPtr *int
	numCPUsPtr = &d.NumCPUs

	vmSpecs := *vm.VM.VmSpecSection

	vmSpecs.NumCpus = numCPUsPtr
	vmSpecs.NumCoresPerSocket = numCPUsPtr
	vmSpecs.MemoryResourceMb.Configured = int64(d.MemorySizeMB)
	vmSpecs.DiskSection.DiskSettings[0].SizeMb = int64(d.DiskSizeMB)

	_, errUpd := vm.UpdateVmSpecSection(&vmSpecs, vm.VM.Description)
	if errUpd != nil {
		log.Errorf("Create.UpdateVmSpecSection error: %v", errUpd)
		return errUpd
	}

	guestSection, err := d.prepareCustomSectionForVM(*vm.VM.GuestCustomizationSection)
	if err != nil {
		log.Errorf("Create.prepareCustomSectionForVM error: %v", err)
		return err
	}

	_, errSet := vm.SetGuestCustomizationSection(&guestSection)
	if errSet != nil {
		log.Errorf("Create.SetGuestCustomizationSection error: %v", errSet)
		return errSet
	}

	return nil
}

func (d *Driver) prepareCustomSectionForVM(vmScript types.GuestCustomizationSection) (types.GuestCustomizationSection, error) {
	sshKey, errSsh := d.createSSHKey()
	if errSsh != nil {
		log.Errorf("prepareCustomSection.createSSHKey error: %s", errSsh)
		return types.GuestCustomizationSection{}, errSsh
	}

	var (
		section      types.GuestCustomizationSection
		adminEnabled bool
		scriptSh     string
	)

	section = vmScript

	section.ComputerName = d.MachineName
	section.AdminPasswordEnabled = &adminEnabled

	scriptSh = d.InitData + "\n"
	// append ssh user to script
	scriptSh += "\nuseradd -m -d /home/" + d.SSHUser + " -s /bin/bash " + d.SSHUser + "\nmkdir -p /home/" + d.SSHUser + "/.ssh\nchown -R " + d.SSHUser + ":" + d.SSHUser + " /home/" + d.SSHUser + "/.ssh\nchmod 700 /home/" + d.SSHUser + "/.ssh\nchmod 600 /home/" + d.SSHUser + "/.ssh/authorized_keys\nusermod -a -G sudo " + d.SSHUser + "\necho \"" + strings.TrimSpace(sshKey) + "\" > /home/" + d.SSHUser + "/.ssh/authorized_keys\necho \"" + d.SSHUser + "     ALL=(ALL) NOPASSWD:ALL\" >>  /etc/sudoers\nswapoff -a\n"

	// resize rootFS for ubuntu*
	if strings.HasPrefix(d.CatalogItem, ubuntu20) {
		scriptSh += "\nrm -rf /swap.img\n"
		scriptSh += "\ngrowpart /dev/sda 3\npvresize /dev/sda3\nlvextend -l 100%VG /dev/mapper/ubuntu--vg-ubuntu--lv\nresize2fs /dev/mapper/ubuntu--vg-ubuntu--lv\n"
		scriptSh += "\nuserdel -r ubuntu || echo true\n"
	}
	if strings.HasPrefix(d.CatalogItem, ubuntu18) {
		scriptSh += "\nrm -rf /swapfile\n"
		scriptSh += "\napt update\n"
		scriptSh += "\nDEBIAN_FRONTEND=noninteractive apt -y install cloud-guest-utils curl open-iscsi\ngrowpart /dev/sda 2\nresize2fs /dev/sda2\n"
		scriptSh += "\nuserdel -r ubuntu || echo true\n"
	}

	scriptSh += "\nsed -i 's/.*PermitRootLogin.*/PermitRootLogin no/g' /etc/ssh/sshd_config\n"
	scriptSh += "\nsed -i 's/.*PasswordAuthentication.*/PasswordAuthentication no/g' /etc/ssh/sshd_config\n"
	scriptSh += "\nservice sshd restart\n"

	if d.Rke2 {
		// if rke2
		readUserData, errRead := os.ReadFile(d.UserData)
		if errRead != nil {
			log.Errorf("prepareCustomSection.ReadFile error: %s", errRead)
			return types.GuestCustomizationSection{}, errRead
		}

		cloudInit := getRancherCloudInit(string(readUserData))

		log.Infof("prepareCustomSection ----> rke2: %v Generate /usr/local/custom_script/install.sh file", d.Rke2)

		// generate install.sh
		cloudInitWithQuotes := strings.Join([]string{"'", cloudInit, "'"}, "")
		scriptSh += "mkdir -p /usr/local/custom_script\n"
		scriptSh += "echo " + cloudInitWithQuotes + " | base64 -d | gunzip | sudo tee /usr/local/custom_script/install.sh\n"
		scriptSh += "nohup sh /usr/local/custom_script/install.sh > /dev/null 2>&1 &\n"
		scriptSh += "exit 0\n"
	} else {
		// if rke1
		scriptSh += d.InitData
	}

	log.Infof("prepareCustomSection generate script ----> %s", scriptSh)

	section.CustomizationScript = scriptSh

	return section, nil
}

func (d *Driver) deleteMachineError(err error) error {
	log.Infof("deleteMachine reason ----> %w", err)

	if errRemove := d.Remove(); err != nil {
		log.Errorf("deleteMachine %v", errRemove)
		return errRemove
	}

	return err
}
