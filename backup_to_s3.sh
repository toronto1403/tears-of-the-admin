#!/bin/bash
#set -euo pipefail

VERSION="1.5"

### ================= CONFIG =================
RETENTION_COUNT=2
S3_BUCKET="s3://semantika-bucket/pgsql_backups"

TG_BOT_TOKEN="..."
TG_CHAT_ID="..."

BASE_DIR="/var/lib/docker/pgsql_backups"
LOGFILE="$BASE_DIR/pgsql-backup.log"
PGDATA="/var/lib/postgresql/data"
### ==========================================

### =============== FUNCTIONS ===============

usage () {
cat <<EOF
PostgreSQL Docker Backup Script v$VERSION

USAGE:
  $0 [OPTIONS] <container_name> <db_owner>

OPTIONS:
  --restore       Perform restore on existing container (DANGEROUS)
  --no-s3-retention    Disable S3 retention cleanup
  -h, --help           Show this help
  -v, --version        Show version

EXAMPLE:
  $0 pgsql-prod postgres
  $0 --restore pgsql-prod postgres

NOTES:
  Restore test STOPS the container and temporarily replaces PGDATA.
  Use only during maintenance windows.
EOF
exit 0
}

touch "$LOGFILE"

log () {
    echo "[$(date '+%F %T')] $*" >> "$LOGFILE"
}

telegram_send () {
    curl -s -X POST "https://api.telegram.org/bot${TG_BOT_TOKEN}/sendMessage" \
        -d chat_id="$TG_CHAT_ID" \
        -d text="$1" \
        -d parse_mode="HTML" >/dev/null
}

check_disk () {
    local use
    use=$(df -P "$BASE_DIR" | awk 'NR==2 {gsub(/%/, "", $5); print $5}')
    [[ "$use" -ge 95 ]] && fatal "Disk usage ${use}%"
}

### =============== ARG PARSING ===============

RESTORE=false
S3_RETENTION=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --restore) RESTORE=true ;;
    --no-s3-retention) S3_RETENTION=false ;;
    -h|--help) usage ;;
    -v|--version) echo "$VERSION"; exit 0 ;;
    *) break ;;
  esac
  shift
done

[[ $# -lt 2 ]] && usage

container_name="$1"
db_owner="$2"

timeslot="$(date '+%Y%m%d%H%M')"
backup_dir="$BASE_DIR/${container_name}-${timeslot}"

fatal () {
    log "ERROR: $*"
    telegram_send "❌ <b>PostgreSQL backup FAILED</b>
<b>Container:</b> ${container_name}
<b>Backup dir:</b> <code>${backup_dir}</code>
<b>Error:</b> $*"
    exit 1
}

### =============== BACKUP ===================

pg_stop () {
    docker stop "$container_name"
}

pg_start () {
    docker start "$container_name" 
}

pg_backup () {
    check_disk
    log "Starting backup for $container_name"

    mkdir -p "$backup_dir"

    docker exec -i "$container_name" \
        pg_basebackup -U "$db_owner" -Ft -z -D /tmp/backup \
        || fatal "pg_basebackup failed"

    docker cp "$container_name:/tmp/backup" "$backup_dir" \
        || fatal "docker cp failed"

    docker exec "$container_name" rm -rf /tmp/backup
    log "Local backup completed"
}

### =============== S3 UPLOAD =================

push_to_s3 () {
    aws s3 ls "$S3_BUCKET" >/dev/null 2>&1 || fatal "S3 unreachable"

    aws s3 sync "$BASE_DIR" "$S3_BUCKET" || fatal "S3 upload failed"

    log "Backup uploaded to S3"
}

### =============== SIZE METRICS ==============

get_backup_sizes () {
    local_size_mb=$(du -sm "$backup_dir" | awk '{print $1}')

    s3_size_mb=$(aws s3 ls "$S3_BUCKET/" \
        --recursive | awk '{sum+=$3} END {print int(sum/1024/1024)}')

    log "Backup size: local=${local_size_mb}MB s3=${s3_size_mb}MB"
}

### =============== RESTORE  ==============

rollback_restore () {
    log "Restore failed, rolling back original data"
	
    pg_stop || fatal "Failed to stop PostgreSQL"

    docker cp "/tmp/original_data_${container_name}_${timeslot}/". "$container_name:$PGDATA" || fatal "Failed to restore original data"	

    pg_start || fatal "Failed to start PostgreSQL"

    fatal "Restore failed"
}

restore () {

    last_backup_dir=$(ls -1d "$BASE_DIR/${container_name}-"* 2>/dev/null | sort | tail -n 1)
    [[ -z "$last_backup_dir" ]] && fatal "No backups found for container: $container_name"
	
    log "Starting restore"

    # Stop Postgres inside container
    pg_stop || fatal "Failed to stop PostgreSQL"

    # Backup original PGDATA
	
	mkdir -p /tmp/original_data_${container_name}_${timeslot} &&
    docker cp -a "$container_name:$PGDATA/". /tmp/original_data_${container_name}_${timeslot} || fatal "Failed to save original data"


    # Restore basebackup
	mkdir -p /tmp/restored_data_${container_name}_${timeslot} &&
	tar -xzf "$last_backup_dir/backup/base.tar.gz" -C "/tmp/restored_data_${container_name}_${timeslot}" || fatal "Failed to unzip bakcup base data"
	tar -xzf "$last_backup_dir/backup/pg_wal.tar.gz" -C "/tmp/restored_data_${container_name}_${timeslot}/pg_wal" || fatal "Failed to unzip bakcup pg_wal data"
		
    docker cp "/tmp/restored_data_${container_name}_${timeslot}/". "$container_name:$PGDATA" || rollback_restore

    # Start PostgreSQL
    pg_start || rollback_restore
    sleep 5

    # Validate
    docker exec -u postgres "$container_name" pg_isready || rollback_restore
    docker exec -u postgres "$container_name" psql -U "$db_owner" -c "SELECT 1;" || rollback_restore

    log "Restore test passed"
}


### =============== RETENTION =================

apply_local_retention () {
    log "Applying local retention"
    ls -1d "$BASE_DIR/${container_name}-"* 2>/dev/null | \
        sort | head -n -"$RETENTION_COUNT" | xargs -r rm -rf
}

apply_s3_retention () {
    [[ "$S3_RETENTION" == false ]] && return

    log "Applying S3 retention"
    aws s3 ls "$S3_BUCKET/" | grep db | awk '{print $2}' | \
        grep "^${container_name}-" | sort | \
        head -n -"$RETENTION_COUNT" | while read -r old; do
            aws s3 rm "$S3_BUCKET/$old" --recursive
            log "Removed S3 backup: $old"
        done
}

cleanup_local () {
	rm -rf "/tmp/restored_data_${container_name}"_*
	rm -rf "/tmp/original_data_${container_name}"_*
    log "Current local backup removed"
}

### ================= RUN ====================

$RESTORE && restore && cleanup_local && log "Backup restored successfully" && telegram_send "✅ <b>PostgreSQL ${container_name} restore OK</b> <b>Source backup:</b> <code>${last_backup_dir}</code>" && exit 0

pg_backup
push_to_s3
get_backup_sizes
cleanup_local
apply_local_retention
apply_s3_retention

telegram_send "✅ <b>PostgreSQL backup OK</b>
<b>Container:</b> ${container_name}
<b>Backup dir:</b> <code>${backup_dir}</code>
<b>Local size:</b> ${local_size_mb} MB
<b>S3 size:</b> ${s3_size_mb} MB"

log "Backup job completed successfully"
