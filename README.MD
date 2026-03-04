# PostgreSQL Docker Backup Script

Version: **1.5**

This script is designed for **backing up PostgreSQL databases** running in Docker containers, with support for:

- local backup storage
- uploading backups to S3
- retention policy
- Telegram notifications
- manual restore into a **separately started container**
---

## Features

- Backup via `pg_basebackup` (physical backup)
- Local backup storage
- Uploading backups to S3
- Retention policy (both local and S3)
- Backup size calculation
- Telegram notifications for success / failure
- Restore from the **latest available local backup**
- Automation via `/etc/crontab`:

0 2 * * * root /usr/local/bin/pgsql-docker-backup.sh re-beta-db db-user
10 2 * * * root /usr/local/bin/pgsql-docker-backup.sh re-prod-db db-user
20 2 * * * root /usr/local/bin/pgsql-docker-backup.sh secure-pdf-db secure_pdf_user
---

## Backup Structure

Backups are stored locally in the following directory:

/var/lib/docker/pgsql_backups/<container_name>-YYYYMMDDHHMM/backup/
                                                            ├── base.tar.gz
                                                            └── pg_wal.tar.gz
---

## Usage

### Regular backup

./pgsql-backup.sh <container_name> <db_owner>

### Restore (`--restore`)

./pgsql-backup.sh --restore <container_name> <db_owner>

Restore process:

- Restore is performed from the **latest local backup** in  
  `/var/lib/docker/pgsql_backups`
- The backup is extracted into a temporary directory `/tmp`
- Data is copied into the container’s `$PGDATA`
- PostgreSQL is validated using:

pg_isready
and
SELECT 1;
- If validation fails, the restore is considered unsuccessful.
---

## Retention Policy

### Local storage

The script keeps the last **N backups**:
RETENTION_COUNT=2

Older backup directories are automatically removed.
---

### S3

By default, the same retention logic is applied to S3.
It can be disabled with:
--no-s3-retention
---

## S3 Storage

- Upload is performed using:

aws s3 cp --recursive

- Bucket availability is checked before upload
- Retention is applied based on the backup directory name
---

## Downloading a Backup from S3
To download a backup from S3 to local storage, run:

aws s3 cp s3://semantika-bucket/pgsql_backups/${backup_dir} \
             /var/lib/docker/pgsql_backups/${backup_dir} \
             --recursive


Where **`backup_dir`** is the **Backup dir** value from the Telegram notification.
