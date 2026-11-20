#!/usr/bin/env bash
set -euo pipefail

role="${1:-}"
shift || true
dry=0
force_disk=""
disk="${BENCH_DISK:-/dev/nvme1n1}"
[ -L "$disk" ] && disk="$(readlink -f "$disk")"
remote_dir="${REMOTE_DIR:-/opt/dw-benchmarks}"
mapper=benchcrypt
luks_key=/root/.bench-luks.key
fstab="${FSTAB_FILE:-/etc/fstab}"
crypttab="${CRYPTTAB_FILE:-/etc/crypttab}"
build_user="${SUDO_USER:-$(id -un)}"

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) dry=1 ;;
    --force-disk) force_disk="${2:?--force-disk needs a device}"; shift ;;
    *) echo "unknown argument: $1" >&2; exit 64 ;;
  esac
  shift
done

case "$role" in
  a|b) ;;
  *) echo "usage: setup.sh a|b [--dry-run] [--force-disk DEV]" >&2; exit 64 ;;
esac

run() {
  if [ "$dry" = 1 ]; then
    printf '+ %s\n' "$*"
  else
    "$@"
  fi
}

install_docker() {
  run sh -c 'command -v docker >/dev/null 2>&1 || curl -fsSL https://get.docker.com | sh'
  run usermod -aG docker "${SUDO_USER:-${USER:-root}}"
  run apt-get install -y jq rsync build-essential cryptsetup parted linux-tools-common
}

set_sysctls() {
  run sh -c "printf 'net.ipv4.ip_local_port_range=1024 65535\nnet.core.somaxconn=65535\nnet.ipv4.tcp_max_syn_backlog=65535\nfs.file-max=2097152\n' > /etc/sysctl.d/99-bench.conf"
  run sysctl --system
}

set_limits() {
  run sh -c "printf '* soft nofile 1048576\n* hard nofile 1048576\nroot soft nofile 1048576\nroot hard nofile 1048576\n' > /etc/security/limits.d/99-bench.conf"
}

set_governor() {
  run sh -c 'for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do [ -w "$g" ] && echo performance > "$g"; done; true'
}

part() {
  case "$disk" in
    *[0-9]) echo "${disk}p$1" ;;
    *) echo "$disk$1" ;;
  esac
}

disk_has_partitions() {
  [ "$dry" = 1 ] && return 1
  [ "$(lsblk -n -o NAME "$disk" | wc -l)" -gt 1 ]
}

guard_disk() {
  if disk_has_partitions && [ "$force_disk" != "$disk" ]; then
    echo "refusing: $disk has existing partitions; pass --force-disk $disk to override" >&2
    exit 1
  fi
}

format_disk() {
  guard_disk
  run parted -s "$disk" mklabel gpt mkpart plain ext4 0% 50% mkpart crypt 50% 100%
  run mkfs.ext4 -F "$(part 1)"
  run sh -c "dd if=/dev/urandom of=$luks_key bs=64 count=1 && chmod 400 $luks_key"
  run cryptsetup luksFormat --batch-mode --type luks2 --cipher aes-xts-plain64 --key-size 512 --hash sha256 --perf-no_read_workqueue --perf-no_write_workqueue --key-file "$luks_key" "$(part 2)"
  run cryptsetup open --perf-no_read_workqueue --perf-no_write_workqueue --persistent --key-file "$luks_key" "$(part 2)" "$mapper"
  run mkfs.ext4 -F "/dev/mapper/$mapper"
}

mount_disks() {
  run mkdir -p /data/plain /data/crypt
  run mount "$(part 1)" /data/plain
  run mount "/dev/mapper/$mapper" /data/crypt
}

persist_mounts() {
  run sh -c "touch $crypttab $fstab && sed -i -e '\\#[[:space:]]/data/plain[[:space:]]#d' -e '\\#[[:space:]]/data/crypt[[:space:]]#d' $fstab && sed -i '/^$mapper /d' $crypttab"
  run sh -c "u=\$(blkid -s UUID -o value $(part 1)); [ -n \"\$u\" ] || { echo 'empty UUID for $(part 1)' >&2; exit 1; }; printf 'UUID=%s /data/plain ext4 defaults,nofail 0 2\\n' \"\$u\" >> $fstab"
  run sh -c "u=\$(blkid -s UUID -o value $(part 2)); [ -n \"\$u\" ] || { echo 'empty UUID for $(part 2)' >&2; exit 1; }; printf '$mapper UUID=%s $luks_key luks,no-read-workqueue,no-write-workqueue\\n' \"\$u\" >> $crypttab"
  run sh -c "printf '/dev/mapper/$mapper /data/crypt ext4 defaults,nofail 0 2\\n' >> $fstab"
}

install_harness_build() {
  run chown -R "$build_user:" "$remote_dir"
  run sudo -u "$build_user" -H sh -c 'command -v cargo >/dev/null 2>&1 || [ -x "$HOME/.cargo/bin/cargo" ] || curl -fsSL https://sh.rustup.rs | sh -s -- -y'
  run sudo -u "$build_user" -H sh -c ". \"\$HOME/.cargo/env\" && cargo build --release --manifest-path $remote_dir/harness/Cargo.toml"
}

install_docker
set_sysctls
set_limits
set_governor
if [ "$role" = a ]; then
  format_disk
  mount_disks
  persist_mounts
else
  install_harness_build
fi
