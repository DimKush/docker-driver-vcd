package vmwarevcloud

import (
	"github.com/docker/machine/libmachine/log"
	"github.com/vmware/go-vcloud-director/v2/govcd"
	"github.com/vmware/go-vcloud-director/v2/types/v56"
)

type VCloudClient struct {
	client            *govcd.VCDClient
	virtualDataCenter *govcd.Vdc
	vApp              *govcd.VApp
	org               *govcd.Org
	storageProfileRef types.Reference
	vAppTemplate      govcd.VAppTemplate
	network           *govcd.OrgVDCNetwork
	catalogItem       *govcd.CatalogItem
}

func NewVCloudClient(d *Driver) *VCloudClient {
	// creates a new VCDClient with params
	vcdClient := govcd.NewVCDClient(*d.URL, d.Insecure)

	return &VCloudClient{
		client: vcdClient,
	}
}

func (c *VCloudClient) buildInstance(d *Driver) error {
	log.Infof("buildInstance Connecting vCloud with url %s and name: %s", d.URL, d.UserName)
	// Authenticate to vCloud Director
	errAuth := c.client.Authenticate(d.UserName, d.UserPassword, d.Organization)
	if errAuth != nil {
		log.Errorf("buildVdcApplication.Authenticate error: %v", errAuth)
		return errAuth
	}

	// Prepare vdc application
	org, errOrg := c.client.GetOrgByName(d.Organization)
	if errAuth != nil {
		log.Errorf("buildInstance.GetOrgById error: %v", errOrg)
		return errOrg
	}

	vdc, errName := org.GetVDCByName(d.VDC, true)
	if errName != nil {
		log.Errorf("buildInstance.GetVDCByName error: %v", errName)
		return errName
	}

	log.Infof("Find VDC Network by name: %s", d.OrgVDCNet)

	network, errVdc := vdc.GetOrgVdcNetworkByName(d.OrgVDCNet, true)
	if errVdc != nil {
		log.Errorf("buildInstance.GetOrgVdcNetworkByName error: %v", errVdc)
		return errVdc
	}

	vdc.GetNet

	log.Infof("buildInstance Finding Catalog: %s", d.Catalog)

	catalog, errCat := org.GetCatalogByName(d.Catalog, true)
	if errCat != nil {
		log.Errorf("buildInstance.GetCatalogByName error: %v", errCat)
		return errCat
	}

	log.Infof("buildInstance Finding Catalog item %s", d.CatalogItem)

	catalogItem, errItem := catalog.GetCatalogItemByName(d.CatalogItem, true)
	if errItem != nil {
		log.Errorf("buildInstance.GetCatalogItemByName error: %v", errItem)
		return errItem
	}

	// Get StorageProfileReference
	storageProfileRef, errProf := vdc.FindStorageProfileReference(d.StoreProfile)
	if errProf != nil {
		log.Errorf("buildInstance.FindStorageProfileReference error: %v", errProf)
		return errProf
	}

	vAppTemplate, err := catalogItem.GetVAppTemplate()
	if err != nil {
		log.Errorf("buildInstance.GetVAppTemplate error: %v", err)
		return err
	}

	vAppTemplate.VAppTemplate.Children.VM[0].Name = d.MachineName
	if d.AdapterType != "" {
		log.Infof("Create.postSettingsVM change network to %s...", d.AdapterType)

		log.Infof("Create current network connection is : %+v",
			vAppTemplate.VAppTemplate.Children.VM[0].NetworkConnectionSection.NetworkConnection[0],
		)

		vAppTemplate.VAppTemplate.Children.VM[0].NetworkConnectionSection.NetworkConnection[0] =
			&types.NetworkConnection{
				Network:                 d.OrgVDCNet,
				NetworkAdapterType:      d.AdapterType,
				IPAddressAllocationMode: d.IPAddressAllocationMode,
				NetworkConnectionIndex:  0,
				IsConnected:             true,
				NeedsCustomization:      true,
			}

		log.Infof("Create current network connection is : %+v",
			vAppTemplate.VAppTemplate.Children.VM[0].NetworkConnectionSection.NetworkConnection[0],
		)

	}

	c.vAppTemplate = vAppTemplate
	c.storageProfileRef = storageProfileRef
	c.virtualDataCenter = vdc
	c.catalogItem = catalogItem
	c.network = network

	return nil
}

func (c *VCloudClient) getVDCApp(d *Driver) (*govcd.VApp, error) {
	log.Infof("getVcdStatus Connecting vCloud with url %s and name: %s", d.URL.Path, d.UserName)

	// Authenticate to vCloud Director
	errAuth := c.client.Authenticate(d.UserName, d.UserPassword, d.Organization)
	if errAuth != nil {
		log.Errorf("getVDC.Authenticate error: %v", errAuth)
		return nil, errAuth
	}

	// Prepare vdc application
	org, errOrg := c.client.GetOrgByName(d.Organization)
	if errAuth != nil {
		log.Errorf("getVDC.GetOrgById error: %v", errOrg)
		return nil, errOrg
	}

	vdc, errName := org.GetVDCByName(d.VDC, true)
	if errName != nil {
		log.Errorf("getVDC.GetVDCByName error: %v", errName)
		return nil, errName
	}

	vapp, err := vdc.GetVAppByName(d.MachineName, true)
	if err != nil {
		log.Errorf("getVDC.GetVAppByName error: %v", err)
		return nil, err
	}

	return vapp, nil
}
