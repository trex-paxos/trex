# Setup and running on a five node cluster

This follows the [Jepsen](https://github.com/aphyr/jepsen) approach for setting up a Linux LXC cluster for testing. These instructions assume you want to run everything within a single VirtualBox VM (i.e. works for Mac, Windows or linux). 

1. [optional] Install VirtualBox (or your favourite virtual machine software) 
2. Download debian-8.2.0-amd64-CD-1.iso
3. Install guest debian OS be sure to assign it a big dynamic disk (with expand on demand) and a lot more more RAM and CPUs than defaults. 
4. `su - ; vi /etc/apt/sources.list` append `deb http://http.debian.net/debian jessie-backports main` then `apt-get update`
5. `apt-get install openjdk-8-jdk openjdk-8-jre-headless libjna-java` so you can run client java on this machine
6. `aptitude install lxc bridge-utils libvirt-bin debootstrap dnsmasq` so you can run lxc linux containers as paxos nodes
7. `apt-get install curl dkms clusterssh sudo resolvconf vim` minimal set of tools to help run the cluster including `cssh`
8. `ssh-keygen` as you need an ssh key to login to the cluster nodes
9. Follow the instructions at https://github.com/trex-paxos/jepsen/blob/master/doc/lxc.md
