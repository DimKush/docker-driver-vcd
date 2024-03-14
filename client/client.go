package client

import (
	"net/url"

	"github.com/docker/machine/libmachine/log"
	"github.com/vmware/go-vcloud-director/v2/govcd"
	"github.com/vmware/go-vcloud-director/v2/types/v56"
)

type ConfigClient struct {
	MachineName             string
	UserName                string
	UserPassword            string
	Org                     string
	VDC                     string
	OrgVDCNet               string
	Catalog                 string
	CatalogItem             string
	StorProfile             string
	AdapterType             string
	IPAddressAllocationMode string
	Url                     *url.URL
	Insecure                bool
}

type VCloudClient struct {
	cfg               ConfigClient
	Client            *govcd.VCDClient
	VirtualDataCenter *govcd.Vdc
	Org               *govcd.Org
	StorageProfileRef types.Reference
	VAppTemplate      govcd.VAppTemplate
	Network           *govcd.OrgVDCNetwork
	CatalogItem       *govcd.CatalogItem
}

func NewVCloudClient(cfg ConfigClient) *VCloudClient {
	// creates a new VCDClient with params
	vcdClient := govcd.NewVCDClient(*cfg.Url, cfg.Insecure)

	return &VCloudClient{
		cfg:    cfg,
		Client: vcdClient,
	}
}

func (c *VCloudClient) BuildInstance() error {
	log.Infof("buildInstance Connecting vCloud with url %s and name: %s", c.cfg.Url, c.cfg.UserName)
	// Authenticate to vCloud Director
	errAuth := c.Client.Authenticate(c.cfg.UserName, c.cfg.UserPassword, c.cfg.Org)
	if errAuth != nil {
		log.Errorf("buildVdcApplication.Authenticate error: %v", errAuth)
		return errAuth
	}

	// Prepare vdc application
	org, errOrg := c.Client.GetOrgByName(c.cfg.Org)
	if errAuth != nil {
		log.Errorf("buildInstance.GetOrgById error: %v", errOrg)
		return errOrg
	}

	vdc, errName := org.GetVDCByName(c.cfg.VDC, true)
	if errName != nil {
		log.Errorf("buildInstance.GetVDCByName error: %v", errName)
		return errName
	}

	log.Infof("Find VDC Network by name: %s", c.cfg.OrgVDCNet)

	network, errVdc := vdc.GetOrgVdcNetworkByName(c.cfg.OrgVDCNet, true)
	if errVdc != nil {
		log.Errorf("buildInstance.GetOrgVdcNetworkByName error: %v", errVdc)
		return errVdc
	}

	log.Infof("buildInstance Finding Catalog: %s", c.cfg.Catalog)

	catalog, errCat := org.GetCatalogByName(c.cfg.Catalog, true)
	if errCat != nil {
		log.Errorf("buildInstance.GetCatalogByName error: %v", errCat)
		return errCat
	}

	log.Infof("buildInstance Finding Catalog item %s", c.cfg.CatalogItem)

	catalogItem, errItem := catalog.GetCatalogItemByName(c.cfg.CatalogItem, true)
	if errItem != nil {
		log.Errorf("buildInstance.GetCatalogItemByName error: %v", errItem)
		return errItem
	}

	// Get StorageProfileReference
	storageProfileRef, errProf := vdc.FindStorageProfileReference(c.cfg.StorProfile)
	if errProf != nil {
		log.Errorf("buildInstance.FindStorageProfileReference error: %v", errProf)
		return errProf
	}

	vAppTemplate, err := catalogItem.GetVAppTemplate()
	if err != nil {
		log.Errorf("buildInstance.GetVAppTemplate error: %v", err)
		return err
	}

	vAppTemplate.VAppTemplate.Children.VM[0].Name = c.cfg.MachineName

	log.Infof("Create.postSettingsVM change network to %s...", c.cfg.AdapterType)

	vAppTemplate.VAppTemplate.Children.VM[0].NetworkConnectionSection.NetworkConnection[0] =
		&types.NetworkConnection{
			Network:                 c.cfg.OrgVDCNet,
			NetworkAdapterType:      c.cfg.AdapterType,
			IPAddressAllocationMode: c.cfg.IPAddressAllocationMode,
			NetworkConnectionIndex:  0,
			IsConnected:             true,
			NeedsCustomization:      true,
		}

	c.VAppTemplate = vAppTemplate
	c.StorageProfileRef = storageProfileRef
	c.VirtualDataCenter = vdc
	c.CatalogItem = catalogItem
	c.Network = network

	return nil
}
