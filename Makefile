export GO111MODULE=on
export GOFLAGS=-mod=vendor

OUT := ./bin/docker-machine-driver-vcd
DOCKER_RELEASE_URL := https://github.com/docker/machine/releases/download/v0.16.2/docker-machine
VCD_HREF_URL :=
VCD_CATALOG :=
VCD_CATALOG_ITEM =
VCD_ORG =
VCD_ORGVDCNETWORK =
VCD_STORPROFILE =
VCD_USERNAME =
VCD_PASSWORD =
VCD_VDC =
VCD_VAPP_NAME =
VCD_MACHINE_NAME =

clean:
	rm -rf ./bin/*
.PHONY: clean

prepare:
	curl -L -$(DOCKER_RELEASE_URL)-`uname -s`-`uname -m` >./bin/docker-machine && chmod +x ./bin/docker-machine
.PHONY: prepare

build: clean prepare
	go build -o $(OUT) ./cmd/main.go
.PHONY: build

run: build
	./bin/docker-machine create --driver vcd --vcd-href=$(VCD_HREF_URL) --vcd-catalog=$(VCD_CATALOG) --vcd-catalogitem=$(VCD_CATALOG_ITEM) --vcd-org=$(VCD_ORG) --vcd-orgvdcnetwork=$(VCD_ORGVDCNETWORK) --vcd-password=$(VCD_PASSWORD) --vcd-storprofile=$(VCD_STORPROFILE) --vcd-username=$(VCD_USERNAME) --vcd-vdc=$(VCD_VDC) --vcd-vapp-name=$(VCD_VAPP_NAME) $(VCD_MACHINE_NAME)
.PHONY: run