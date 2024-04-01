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

func NewVCloudClient(cfg ConfigClient) (*VCloudClient, error) {
	log.Infof("NewVCloudClient Connecting vCloud with url %s, name: %s and org: %s", cfg.Url, cfg.UserName, cfg.Org)

	// creates a new VCDClient with params
	client := govcd.NewVCDClient(*cfg.Url, cfg.Insecure)

	vcdClient := &VCloudClient{
		cfg:    cfg,
		Client: client,
	}

	// Authenticate to vCloud Director
	errAuth := vcdClient.Client.Authenticate(cfg.UserName, cfg.UserPassword, cfg.Org)
	if errAuth != nil {
		log.Errorf("NewVCloudClient.Authenticate error: %v", errAuth)
		return nil, errAuth
	}

	// Prepare vdc application
	org, errOrg := vcdClient.Client.GetOrgByName(cfg.Org)
	if errAuth != nil {
		log.Errorf("buildInstance.GetOrgById error: %v", errOrg)
		return nil, errOrg
	}

	vdc, errName := org.GetVDCByName(cfg.VDC, true)
	if errName != nil {
		log.Errorf("buildInstance.GetVDCByName error: %v", errName)
		return nil, errName
	}

	vcdClient.VirtualDataCenter = vdc
	vcdClient.Org = org

	return vcdClient, nil
}

func (c *VCloudClient) BuildInstance() error {
	log.Infof("BuildInstance running with config: %+v", c.cfg)

	network, errVdc := c.VirtualDataCenter.GetOrgVdcNetworkByName(c.cfg.OrgVDCNet, true)
	if errVdc != nil {
		log.Errorf("buildInstance.GetOrgVdcNetworkByName error: %v", errVdc)
		return errVdc
	}

	log.Infof("buildInstance Finding Catalog: %s", c.cfg.Catalog)

	catalog, errCat := c.Org.GetCatalogByName(c.cfg.Catalog, true)
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
	storageProfileRef, errProf := c.VirtualDataCenter.FindStorageProfileReference(c.cfg.StorProfile)
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
	c.CatalogItem = catalogItem
	c.Network = network

	return nil
}
