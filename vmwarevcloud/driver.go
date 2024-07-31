package vmwarevcloud

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/DimKush/docker-driver-vcd/client"
	processor "github.com/DimKush/docker-driver-vcd/processor"
	"github.com/docker/machine/libmachine/drivers"
	log "github.com/docker/machine/libmachine/log"
	"github.com/docker/machine/libmachine/mcnflag"
	"github.com/docker/machine/libmachine/ssh"
	"github.com/docker/machine/libmachine/state"
)

type Driver struct {
	*drivers.BaseDriver
	UserName                string
	UserPassword            string
	VDC                     string
	OrgVDCNet               string
	EdgeGateway             string
	VdcEdgeGateway          string
	PublicIP                string
	PrivateIP               string
	Catalog                 string
	CatalogItem             string
	StorProfile             string
	UserData                string
	InitData                string
	AdapterType             string
	IPAddressAllocationMode string
	DockerPort              int
	CPUCount                int
	MemorySize              int
	DiskSize                int
	VAppID                  string
	Href                    string
	Url                     *url.URL
	Org                     string
	Insecure                bool
	Rke2                    bool
	VAppName                string
	VMachineID              string
	RootAuth                bool
}

func NewDriver(hostName, storePath string) drivers.Driver {
	driver := &Driver{
		VAppName:                defaultVAppName,
		Catalog:                 defaultCatalog,
		CatalogItem:             defaultCatalogItem,
		CPUCount:                defaultCpus,
		MemorySize:              defaultMemory,
		DiskSize:                defaultDisk,
		DockerPort:              defaultDockerPort,
		Insecure:                defaultInsecure,
		Rke2:                    defaultRke2,
		AdapterType:             defaultAdapterType,
		RootAuth:                defaultRootAuth,
		IPAddressAllocationMode: defaultIPAddressAllocationMode,
		BaseDriver: &drivers.BaseDriver{
			SSHPort:     defaultSSHPort,
			MachineName: hostName,
			StorePath:   storePath,
		},
	}

	return driver
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
		mcnflag.StringFlag{
			EnvVar: "VCD_VAPP_NAME",
			Name:   "vcd-vapp-name",
			Usage:  "Vapp name where VM will be create",
			Value:  defaultVAppName,
		},
		mcnflag.BoolFlag{
			EnvVar: "VCD_ROOT_AUTH",
			Name:   "vcd-root-auth",
			Usage:  "Create VM with root password in tty",
		},
	}
}

func (d *Driver) SetConfigFromFlags(flags drivers.DriverOptions) error {

	d.UserName = flags.String("vcd-username")
	d.UserPassword = flags.String("vcd-password")
	d.VDC = flags.String("vcd-vdc")
	d.Org = flags.String("vcd-org")
	d.Href = flags.String("vcd-href")
	d.Insecure = flags.Bool("vcd-insecure")
	d.Rke2 = flags.Bool("vcd-rke2")
	d.PublicIP = flags.String("vcd-publicip")
	d.StorProfile = flags.String("vcd-storprofile")
	d.UserData = flags.String("vcd-user-data")
	d.InitData = flags.String("vcd-init-data")
	d.AdapterType = flags.String("vcd-networkadaptertype")
	d.IPAddressAllocationMode = flags.String("vcd-ipaddressallocationmode")
	d.RootAuth = flags.Bool("vcd-root-auth")
	d.SetSwarmConfigFromFlags(flags)

	// Check for required Params
	if d.UserName == "" || d.UserPassword == "" || d.Href == "" || d.VDC == "" || d.Org == "" || d.StorProfile == "" {
		return fmt.Errorf("please specify vclouddirector mandatory params using options: -vcd-username -vcd-password -vcd-vdc -vcd-href -vcd-org and -vcd-storprofile")
	}

	u, err := url.ParseRequestURI(d.Href)
	if err != nil {
		return fmt.Errorf("Unable to pass url: %s", err)
	}
	d.Url = u

	// If the Org VDC Network is empty, set it to the default routed network.
	if flags.String("vcd-orgvdcnetwork") == "" {
		d.OrgVDCNet = flags.String("vcd-vdc") + "-default-routed"
	} else {
		d.OrgVDCNet = flags.String("vcd-orgvdcnetwork")
	}

	// If the Edge Gateway is empty, just set it to the default edge gateway.
	// if flags.String("vcd-edgegateway") == "" {
	// 	d.EdgeGateway = flags.String("vcd-org")
	// } else {
	d.EdgeGateway = flags.String("vcd-edgegateway")
	// }

	d.VdcEdgeGateway = flags.String("vcd-vdcedgegateway")

	d.Catalog = flags.String("vcd-catalog")
	d.CatalogItem = flags.String("vcd-catalogitem")

	d.DockerPort = flags.Int("vcd-docker-port")
	d.SSHUser = flags.String("vcd-ssh-user")
	d.SSHPort = flags.Int("vcd-ssh-port")
	d.CPUCount = flags.Int("vcd-cpu-count")
	d.MemorySize = flags.Int("vcd-memory-size")
	d.DiskSize = flags.Int("vcd-disk-size")
	d.VAppName = flags.String("vcd-vapp-name")
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

	configVCDClient := d.buildVCDClientConfig()
	vcdClient, err := client.NewVCloudClient(configVCDClient)
	if err != nil {
		log.Errorf("Driver.GetState.NewVCloudClient error: %v", err)
		return state.Error, err
	}

	// creates Processor
	processorConfig := processor.ConfigProcessor{
		VAppName:       d.VAppName,
		VMachineName:   d.BaseDriver.GetMachineName(),
		CPUCount:       d.CPUCount,
		MemorySize:     int64(d.MemorySize),
		DiskSize:       int64(d.DiskSize),
		EdgeGateway:    d.EdgeGateway,
		PublicIP:       d.PublicIP,
		VdcEdgeGateway: d.VdcEdgeGateway,
		Org:            d.Org,
		VAppID:         d.VAppID,
		VMachineID:     d.VMachineID,
	}

	proc := processor.NewVMProcessor(vcdClient, processorConfig)

	return proc.GetState()
}

func (d *Driver) Create() error {
	log.Info("Create() running")

	// create ssh key
	sshKey, errSsh := d.createSSHKey()
	if errSsh != nil {
		log.Errorf("Create().createSSHKey error: %s", errSsh)
		return errSsh
	}

	configVCDClient := d.buildVCDClientConfig()
	vcdClient, err := client.NewVCloudClient(configVCDClient)
	if err != nil {
		log.Errorf("Create().NewVCloudClient error: %v", err)
		return err
	}

	errBuild := vcdClient.BuildInstance()
	if errBuild != nil {
		log.Errorf("Create.buildInstance vdc error: %v", errBuild)
		return errBuild
	}

	log.Info("Create().VCloudClient Set up VApp before running")

	// custom config for script
	confCustom := processor.CustomScriptConfigVMProcessor{
		VAppName:    d.VAppName,
		MachineName: d.BaseDriver.GetMachineName(),
		SSHKey:      sshKey,
		SSHUser:     d.SSHUser,
		UserData:    d.UserData,
		InitData:    d.InitData,
		Rke2:        d.Rke2,
		RootAuth:    d.RootAuth,
	}

	// creates Processor
	processorConfig := processor.ConfigProcessor{
		VAppName:       d.VAppName,
		VMachineName:   d.BaseDriver.GetMachineName(),
		CPUCount:       d.CPUCount,
		MemorySize:     int64(d.MemorySize),
		DiskSize:       int64(d.DiskSize),
		EdgeGateway:    d.EdgeGateway,
		PublicIP:       d.PublicIP,
		VdcEdgeGateway: d.VdcEdgeGateway,
		Org:            d.Org,
	}

	proc := processor.NewVMProcessor(vcdClient, processorConfig)

	vApp, errVApp := proc.Create(confCustom)
	if errVApp != nil {
		log.Errorf("Create.CreateVAppWithVM error: %v", errVApp)
		return errVApp
	}

	virtualMachine, err := vApp.GetVMByName(d.MachineName, true)
	if err != nil {
		log.Errorf("Create.GetVMByName error: %v", err)
		return err
	}

	task, errPowerOn := virtualMachine.PowerOn()
	if errPowerOn != nil {
		log.Errorf("Create.PowerOn error: %v", errPowerOn)
		return errPowerOn
	}

	if errTask := task.WaitTaskCompletion(); errTask != nil {
		log.Errorf("Create.PowerOn.WaitTaskCompletion error: %v", errTask)
		return errTask
	}

	for {
		vm, errVM := vApp.GetVMByName(d.MachineName, true)
		if errVM != nil {
			log.Errorf("Create.GetVMByName error: %v", errVM)
			return errVM
		}

		time.Sleep(2 * time.Second)

		if vm.VM.NetworkConnectionSection.NetworkConnection[0].IPAddress != "" {
			d.PrivateIP = vm.VM.NetworkConnectionSection.NetworkConnection[0].IPAddress
			d.VMachineID = vm.VM.ID
			break
		}
	}

	d.VAppID = vApp.VApp.ID

	ip, errIP := d.GetIP()
	if errIP != nil {
		log.Errorf("Create.GetIP error: %v", errIP)

		return errIP
	}

	d.IPAddress = ip

	return nil
}

func (d *Driver) Start() error {
	log.Info("Start() running")

	// check vcd platform state
	configVCDClient := d.buildVCDClientConfig()
	vcdClient, err := client.NewVCloudClient(configVCDClient)
	if err != nil {
		log.Errorf("Start().NewVCloudClient error: %v", err)
		return err
	}

	// creates Processor
	processorConfig := processor.ConfigProcessor{
		VAppName:       d.VAppName,
		VMachineName:   d.BaseDriver.GetMachineName(),
		CPUCount:       d.CPUCount,
		MemorySize:     int64(d.MemorySize),
		DiskSize:       int64(d.DiskSize),
		EdgeGateway:    d.EdgeGateway,
		PublicIP:       d.PublicIP,
		VdcEdgeGateway: d.VdcEdgeGateway,
		Org:            d.Org,
		VAppID:         d.VAppID,
		VMachineID:     d.VMachineID,
	}

	proc := processor.NewVMProcessor(vcdClient, processorConfig)

	if err := proc.Start(); err != nil {
		log.Errorf("Kill error: %v", err)
		return err
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

	configVCDClient := d.buildVCDClientConfig()
	vcdClient, err := client.NewVCloudClient(configVCDClient)
	if err != nil {
		log.Errorf("Stop.NewVCloudClient error %v", err)
		return err
	}

	log.Info("Stop.VCloudClient.getVDCApp")

	// creates Processor
	processorConfig := processor.ConfigProcessor{
		VAppName:       d.VAppName,
		VMachineName:   d.BaseDriver.GetMachineName(),
		CPUCount:       d.CPUCount,
		MemorySize:     int64(d.MemorySize),
		DiskSize:       int64(d.DiskSize),
		EdgeGateway:    d.EdgeGateway,
		PublicIP:       d.PublicIP,
		VdcEdgeGateway: d.VdcEdgeGateway,
		Org:            d.Org,
		VAppID:         d.VAppID,
		VMachineID:     d.VMachineID,
	}

	proc := processor.NewVMProcessor(vcdClient, processorConfig)
	if err := proc.Stop(); err != nil {
		log.Errorf("Stop error: %v", err)
		return err
	}

	return nil
}

func (d *Driver) Restart() error {
	log.Info("Restart() running")

	vcdClient, err := client.NewVCloudClient(d.buildVCDClientConfig())
	if err != nil {
		log.Errorf("Restart.NewVCloudClient error %v", err)
		return err
	}

	log.Info("Restart.VCloudClient create new processor")

	// creates Processor
	processorConfig := processor.ConfigProcessor{
		VAppName:       d.VAppName,
		VMachineName:   d.BaseDriver.GetMachineName(),
		CPUCount:       d.CPUCount,
		MemorySize:     int64(d.MemorySize),
		DiskSize:       int64(d.DiskSize),
		EdgeGateway:    d.EdgeGateway,
		PublicIP:       d.PublicIP,
		VdcEdgeGateway: d.VdcEdgeGateway,
		Org:            d.Org,
		VAppID:         d.VAppID,
		VMachineID:     d.VMachineID,
	}

	proc := processor.NewVMProcessor(vcdClient, processorConfig)
	if err := proc.Restart(); err != nil {
		log.Errorf("Stop error: %v", err)
		return err
	}

	return nil
}

func (d *Driver) Remove() error {
	log.Info("Remove() running")

	configVCDClient := d.buildVCDClientConfig()
	vcdClient, err := client.NewVCloudClient(configVCDClient)
	if err != nil {
		log.Errorf("Remove.NewVCloudClient error: %v", err)
		return err
	}

	// creates Processor
	processorConfig := processor.ConfigProcessor{
		VAppName:       d.VAppName,
		VMachineName:   d.BaseDriver.GetMachineName(),
		CPUCount:       d.CPUCount,
		MemorySize:     int64(d.MemorySize),
		DiskSize:       int64(d.DiskSize),
		EdgeGateway:    d.EdgeGateway,
		PublicIP:       d.PublicIP,
		VdcEdgeGateway: d.VdcEdgeGateway,
		Org:            d.Org,
		VAppID:         d.VAppID,
		VMachineID:     d.VMachineID,
	}

	var proc processor.Processor

	// if VMachineID is empty, it's a VApp
	if processorConfig.VMachineID == "" {
		processorConfig.VAppName = d.MachineName
		processorConfig.VMachineName = d.MachineName

		proc = processor.NewVAppProcessor(vcdClient, processorConfig)
	} else {
		proc = processor.NewVMProcessor(vcdClient, processorConfig)
	}

	if err := proc.Remove(); err != nil {
		log.Errorf("Remove error: %v", err)
		return err
	}

	return nil
}

func (d *Driver) Kill() error {
	log.Info("Kill() running")

	configVCDClient := d.buildVCDClientConfig()
	vcdClient, err := client.NewVCloudClient(configVCDClient)
	if err != nil {
		log.Errorf("Kill.NewVCloudClient error: %v", err)
		return err
	}

	// creates Processor
	processorConfig := processor.ConfigProcessor{
		VAppName:       d.VAppName,
		VMachineName:   d.BaseDriver.GetMachineName(),
		CPUCount:       d.CPUCount,
		MemorySize:     int64(d.MemorySize),
		DiskSize:       int64(d.DiskSize),
		EdgeGateway:    d.EdgeGateway,
		PublicIP:       d.PublicIP,
		VdcEdgeGateway: d.VdcEdgeGateway,
		Org:            d.Org,
		VAppID:         d.VAppID,
		VMachineID:     d.VMachineID,
	}

	var proc processor.Processor

	if processorConfig.VMachineID == "" {
		proc = processor.NewVAppProcessor(vcdClient, processorConfig)
	} else {
		proc = processor.NewVMProcessor(vcdClient, processorConfig)
	}

	if err := proc.Kill(); err != nil {
		log.Errorf("Kill error: %v", err)
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

func (d *Driver) buildVCDClientConfig() client.ConfigClient {
	return client.ConfigClient{
		MachineName:             d.MachineName,
		UserName:                d.UserName,
		UserPassword:            d.UserPassword,
		Org:                     d.Org,
		VDC:                     d.VDC,
		OrgVDCNet:               d.OrgVDCNet,
		Catalog:                 d.Catalog,
		CatalogItem:             d.CatalogItem,
		StorProfile:             d.StorProfile,
		AdapterType:             d.AdapterType,
		IPAddressAllocationMode: d.IPAddressAllocationMode,
		Url:                     d.Url,
		Insecure:                d.Insecure,
	}
}
