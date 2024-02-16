package cmd

import (
	"github.com/docker/machine/libmachine/drivers/plugin"
	"github.com/DimKush/docker-driver-vcd/vmwarevcloud"

func main() {
	plugin.RegisterDriver(vmwarevcloud.NewDriverVDCloud("", ""))
}
