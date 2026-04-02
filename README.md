# FastNTFS

FastNTFS is a Windows NTFS file search daemon written in Go, backed by the USN journal and SQLite.

## Features

- Full snapshot build from NTFS USN records
- Incremental daemon updates from USN journal
- Local HTTP search API and CLI search command
- Windows scheduled-task based service install/uninstall

## Requirements

- Windows (Administrator required for USN operations)
- Go 1.24+
- NTFS volumes

## Quick Start

```powershell
cd C:\Users\MECHREV\projects\FastNTFS
go run . rebuild --volumes D --db C:\data\fastntfs.db
go run . serve --volumes D --db C:\data\fastntfs.db --addr 127.0.0.1:7788 --flush-seconds 10
go run . search --addr http://127.0.0.1:7788 --query test --match name --type file --limit 20
```

## Install as Service

```powershell
cd C:\Users\MECHREV\projects\FastNTFS
.\install_service.ps1 -DbPath C:\data\fastntfs.db
```

Default behavior:

- Auto-detect and include all local NTFS volumes
- Build binary to `~/bin/fast-ntfs.exe`
- Rebuild full index before service starts

Optional flags:

- `-Volumes D,E`
- `-SkipRebuild`
- `-MaxRecords 50000000`

Uninstall:

```powershell
.\uninstall_service.ps1
```
