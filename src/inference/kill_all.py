import subprocess

import yaml

SLURM_YML_PATH = "slurm.yaml"
# safer pkill patterns
COMMAND_TO_RUN = "pkill -9 -f dragon; pkill -9 -f python3"


def read_hostnames(yml_path):
    with open(yml_path) as f:
        data = yaml.safe_load(f)
    hosts = []
    for node_data in data.values():
        host = node_data.get("host_name") or node_data.get("name")
        if host and host not in hosts:  # avoid duplicates
            hosts.append(host)
    return hosts


def ssh_and_kill(host):
    ssh_cmd = f"ssh -o BatchMode=yes -o StrictHostKeyChecking=no {host} '{COMMAND_TO_RUN}'"
    print(f"[INFO] Killing on {host}...")
    try:
        result = subprocess.run(ssh_cmd, shell=True, check=True, capture_output=True)
        print(f"[SUCCESS] {host}: {result.stdout.decode().strip()}")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] {host}: {e.stderr.decode().strip()}")


def main():
    hosts = read_hostnames(SLURM_YML_PATH)
    for host in hosts:
        ssh_and_kill(host)


if __name__ == "__main__":
    main()
