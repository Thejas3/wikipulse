# Deploying to an always-on VM

Target: any Ubuntu 24.04 VM with at least **12 GB RAM**, e.g. Oracle Cloud Always Free
**VM.Standard.A1.Flex** (ARM). As of mid-2026 that allowance is 2 OCPUs / 12 GB, down from
4 / 24; check *Governance → Limits* for your account. Every image in the stack is multi-arch,
so ARM and x86 both work.

## 1. Create the VM

Image `Canonical-Ubuntu-24.04-Minimal-aarch64` (on ARM shapes the plain image may not be
listed; Minimal is fine), a public IPv4 address, your SSH public key, and a 100 GB boot volume.
"Out of capacity" is common on the free ARM shape: retry later or try another availability
domain.

## 2. Copy the code and secrets (from your machine)

Secrets travel over SSH only; they are never committed.

```bash
VM=ubuntu@<VM_IP>
ssh $VM 'git clone https://github.com/Thejas3/wikipulse.git'
scp .env $VM:wikipulse/.env
scp -r secrets $VM:wikipulse/secrets   # deploy key for dashboard publishing
```

## 3. Provision and start

```bash
ssh $VM 'bash wikipulse/deploy/provision.sh'
```

## 4. Hand over from the laptop

Only one machine should publish the dashboard. Once the VM is healthy (`make audit-quick` on
the VM reports `ok`), stop the laptop stack with `make down`. Bronze and Silver start empty on
the VM, and the producer begins from "now", so the laptop's history stays on the laptop.

## Sizing for 2 OCPUs

On 2 cores, the Spark app (`streaming`) is the bottleneck. If end-to-end latency (Grafana, or
`make latency`) goes over 60 s at p95, in order of impact:

1. `SPARK_MASTER=local[2]` in `.env` (one task slot per core).
2. Run Airflow on a laptop and only the streaming path on the VM (PRD §9 fallback).
3. Raise `DOWNSTREAM_TRIGGER_INTERVAL` to `5 seconds` to batch Silver and Gold work.
