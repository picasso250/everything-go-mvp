# FastNTFS Runbook

## 0) Admin terminal required
All volume USN operations should run in an elevated terminal.

## 1) Full snapshot build

```powershell
cd C:\Users\MECHREV\projects\FastNTFS
go run . rebuild --volumes C,D --max-records 50000000 --db C:\Users\MECHREV\projects\everything-rs-mvp\everything_mvp_go_bench.db
```

## 2) Start daemon (manual)

```powershell
cd C:\Users\MECHREV\projects\FastNTFS
go run . serve --volumes C,D --db C:\Users\MECHREV\projects\everything-rs-mvp\everything_mvp_go_bench.db --addr 127.0.0.1:7788 --flush-seconds 10
```

## 3) Search

```powershell
cd C:\Users\MECHREV\projects\FastNTFS
go run . search --addr http://127.0.0.1:7788 --query dota --match name --type file --limit 20
go run . search --addr http://127.0.0.1:7788 --query dota --match path --type dir --limit 20
go run . search --addr http://127.0.0.1:7788 --query dota --match all --type all --limit 20
```

## 4) Install as background service (Administrator)

Run in elevated PowerShell:

```powershell
cd C:\Users\MECHREV\projects\FastNTFS
.\install_service.ps1 -Address 127.0.0.1:7788 -FlushSeconds 10 -DbPath C:\Users\MECHREV\projects\everything-rs-mvp\everything_mvp_go_bench.db
```

Default install behavior:
- Auto-detect all local NTFS volumes and include all of them.
- Build binary to `~/bin/fast-ntfs.exe`.
- Run full snapshot rebuild before registering and starting the task.

Optional overrides:
- Set explicit volumes: `-Volumes D,E`
- Skip full rebuild: `-SkipRebuild`
- Limit rebuild scan size: `-MaxRecords 50000000`

Uninstall:

```powershell
cd C:\Users\MECHREV\projects\FastNTFS
.\uninstall_service.ps1
```

## 5) Health checks

```powershell
curl http://127.0.0.1:7788/status
curl -X POST http://127.0.0.1:7788/flush
```

## 6) Common pitfalls

- Install script must run as Administrator.
- If `http://127.0.0.1:7788/status` is unreachable, check whether another process occupies port `7788`.
- If `/status` shows `rebuild_required=true`, run `go run . rebuild ...` once, then restart daemon.
- First install now performs full rebuild by default; pass `-SkipRebuild` only when you explicitly want incremental-only startup.

## Benchmark baseline (2026-04-02)

- Rust full build: `6.45s`, `entries=242072`
- Go full build: `8.038s`, `entries=242072`
- Go full build (post-fix run): `7.26s`, `entries=242072`, `unresolved_parents=24`
