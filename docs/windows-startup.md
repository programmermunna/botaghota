# Running botaghota on Windows

botaghota is pure Python standard library — no dependencies, no build
step. Python 3.10+ from <https://python.org> is all you need (check
"Add python.exe to PATH" during install).

## Run once

```powershell
mkdir C:\botaghota
cd C:\botaghota
# copy botaghota.py here, then:
python botaghota.py https://example.com/ --data-dir .\data --workers 4
```

Graceful stop: `Ctrl+C` once — the crawler finishes the current page,
checkpoints, and exits. (Second Ctrl+C kills it; state is still safe —
the SQLite frontier + inflight table recover on the next start.)

## Start automatically at logon (Task Scheduler)

```powershell
$action = New-ScheduledTaskAction `
    -Execute "python" `
    -Argument "C:\botaghota\botaghota.py https://example.com/ --data-dir C:\botaghota\data --workers 4" `
    -WorkingDirectory "C:\botaghota"
$trigger = New-ScheduledTaskTrigger -AtLogOn
Register-ScheduledTask -TaskName "botaghota" `
    -Action $action -Trigger $trigger `
    -Description "botaghota polite crawler"
```

Or via GUI: Task Scheduler → Create Task → Trigger "At log on" →
Action "Start a program" → `python` with the same arguments.

## Stop / remove

```powershell
Unregister-ScheduledTask -TaskName "botaghota" -Confirm:$false
```

To stop a running crawl gracefully, end the `python` process in Task
Manager — SIGTERM equivalent on Windows closes the process cleanly
enough that the atomic SQLite state survives; the crawler was designed
for `kill -9` anyway.

## Notes

- `config.toml` next to `botaghota.py` is read automatically.
- Logs rotate under `data\logs\`; crawled links land in
  `data\crawled_links\YYYY-MM-DD.txt`.
- Everything runs locally — no services, no cloud, no account.
