package main

import (
	"github.com/DimKush/docker-driver-vcd/vmwarevcloud"
	"github.com/docker/machine/libmachine/drivers/plugin"
)

func main() {
	plugin.RegisterDriver(vmwarevcloud.NewDriverVDCloud("", ""))
}
