# docker-driver-vcd

## Custom vCloud Director driver for docker-machine.

Mandatory parameters:
1) vcd-username
2) vcd-password
3) vcd-vdc vcd tenant
4) vcd-vdcedgegateway vcd tenant for edge gateway
5) vcd-org vcd tenant organization
6) vcd-orgvdcnetwork vdc network to find gateway
7) vcd-edgegateway edge gateway name for publicIP
8) vcd-publicip public ip to attach gateway
9) vcd-catalog
10) vcd-catalogitem
11) vcd-storprofile
12) vcd-href vcd api endpoint (don't forget to includ /api without trailing slash!) ex.: https://vdc.host/api
13) vcd-insecure bool whether to allow insecure connections to vCloud API
14) vcd-cpu-count
15) vcd-memory-size
16) vcd-disk-size
17) vcd-ssh-port
18) vcd-docker-port
19) vcd-ssh-user
20) vcd-user-data bash script