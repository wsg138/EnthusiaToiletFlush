# End-to-end manual verification runbook

Covers implementation.md §1, §5 — the full state cycle of one armed
backend restart plus the dedicated Velocity proxy restart path.

## Prereqs

- Velocity-CTD 3.5.x dev proxy running locally.
- Two Paper 1.21.x backends: `survival`, `creative`.
- One hub backend: `lobby`.
- `queue-restart-velocity-0.1.0-SNAPSHOT.jar` in proxy `plugins/`.
- `queue-restart-paper-companion-0.1.0-SNAPSHOT.jar` in **each** backend's `plugins/`.
- LuckPerms (or any permission plugin) granting:
  - test op: `queuerestart.command.schedrestart`, `queuerestart.command.admin`.
  - test "owner" account: `group.owner` (weight 1000 in default ladder).
  - test "default" account: no rank perms.
- (Optional) CheckHacks-fork installed on backends to verify gate flow —
  pending the PR in `docs/checkhacks-fork-pr.md`.
- **Auto-restart on clean exit must be enabled for every backend and the
  Velocity proxy.** The companion calls `Bukkit.shutdown()` for a backend,
  while the proxy plugin calls `ProxyServer.shutdown(reason)` for Velocity.
  Both JVMs exit cleanly and require a supervisor to relaunch them.
  - **Bloom / Pterodactyl panels:** enable the relevant automatic restart or
    crash-detection behavior for each process, and verify it also relaunches
    a process that exits with code 0.
  - **Local / bare-metal:** wrap each launch command in a loop (see
    `test_net/lobby2/launch-windows.bat` for the Windows pattern).
  - **systemd:** `Restart=always` on each unit.

## 1. Boot smoke test

1. Start hub, both backends, then the proxy.
2. Watch the proxy log for:
   - `[queue-restart] config loaded from plugins/queue-restart/config.yml`
   - no `sound volume` warnings (default suite stays under 0.8).
   - `queue-restart ready` including `proxy-target=proxy` and the expected
     `proxy-schedules=<count>`.
3. Watch each backend log for:
   - `[queue-restart-companion] enabled` and either
     `CheckHacks integration: present` or `CheckHacks integration: absent`.

## 2. Armed → countdown → drain → backend restart

1. As op: `/server survival`, then `/schedrestart 1`.
2. Expect chat: `Server <yellow>survival</yellow> restarts in <yellow>1m</yellow>`
   plus a sound at every configured mark (60s, 30s, 10..1s, T-0).
3. At T-30s (`drain-lead-seconds`): players begin transferring to `lobby`
   in batches of 10 (default), spaced 40 ticks apart.
4. At T-0 the proxy sends `RestartNow` (channel `qrestart:v1`, type 0x10)
   to `survival`. The companion logs `executing restart: <mode> <arg>` and
   the backend goes down.
5. Proxy log: `coordinator(survival): RESTART_SENT`.

## 3. Server down → ping ok → rejoin

1. Proxy polls `survival.ping()` every `ping-poll-seconds` (default 3s).
   Log shows `coordinator(survival): SERVER_DOWN, polling…`.
2. When the backend is back: `coordinator(survival): REJOIN_RELEASE`.
3. Every cohort member that is still online gets enqueued for `survival`
   with their rank weight (`queue` debug logs from VelocityCTD show this).
4. Owner (weight 1000) gets in before default (weight 0).
5. Coordinator returns to IDLE.

## 4. Velocity proxy restart

1. Confirm the proxy's process supervisor is configured to relaunch a clean
   exit before performing this test.
2. From console or an authorized player, run `/schedrestart 1 proxy`.
3. `/schedrestart status` must show `proxy → ARMED` or `proxy → COUNTDOWN`.
4. Every connected player, regardless of backend, receives the configured
   countdown warning and sounds at 60s, 30s, 10..1s.
5. At T-0 every player receives `Proxy restarting now. Reconnect shortly.`
   and is disconnected with the same intent in the kick reason.
6. Proxy log includes `cleanly shutting down Velocity for scheduled proxy restart`.
7. The supervisor relaunches Velocity and players can reconnect.
8. Repeat with `/schedrestart 1 proxy`, then run
   `/schedrestart cancel proxy`; status must no longer contain an active
   proxy countdown and Velocity must remain online past the original T-0.

## 5. Automatic proxy schedule and reload

1. Set a restart time a few minutes ahead in the proxy config:

   ```yaml
   proxy-restart:
     restart-times: ["HH:mm"]
     time-zone: "America/New_York"
     warn-minutes: 1
   ```

2. Run `/qrestart reload`.
3. Run `/qrestart trigger proxy-HH:mm` to confirm the deterministic schedule
   name is registered, then cancel it with `/schedrestart cancel proxy`.
4. Wait for the cron start minute. The proxy countdown must arm one minute
   before the configured shutdown time and reach T-0 at `HH:mm` in the
   configured time zone.
5. After the proxy relaunches, remove the test time and run `/qrestart reload`.
   `/qrestart trigger proxy-HH:mm` must then report an unknown schedule.
6. Confirm backend schedule names remain available after both reloads; the
   proxy-owned schedule refresh must not erase SLP-discovered definitions.

## 6. CheckHacks gate (optional, requires CheckHacks-fork PR)

1. With CheckHacks-fork installed, repeat §2 against `survival`.
2. While players sit on `lobby` waiting to be released, run a CheckHacks
   sign check against one of them.
3. CLEAN result → released into queue.
4. DETECTED result → removed from queue, broadcast suppressed.
5. Wait `check-gate-timeout-seconds` (default 60s) without firing the
   event → released (because `release-on-timeout: true`).

## 7. Negative / safety paths

| Action | Expected |
|---|---|
| `/schedrestart 5 lobby` | rejected — REQ-060 |
| Run `/schedrestart 5` on `survival` twice in a row | second rejected — REQ-061 |
| Run `/schedrestart 5 proxy` twice in a row | second rejected — REQ-023/061 |
| Stop the companion on `survival`, then `/schedrestart 5` | rejected — REQ-062 |
| `/schedrestart cancel` while ARMED | broadcast `restart cancelled` — REQ-005 |
| `/schedrestart cancel proxy` while proxy is IDLE | rejected — REQ-023 |
| `/schedrestart cancel` while backend is IDLE | rejected |
| Player with `queuerestart.bypass.drain` on `survival` during drain | not transferred — REQ-014 |
| Player with `queuerestart.bypass.checkhacks` after restart | released without waiting — REQ-043 |
| `/qrestart reload` mid-countdown | reload OK, countdown unaffected — REQ-050 |
| `/qrestart trigger proxy-HH:mm` after removing that time | rejected as unknown schedule — REQ-023/051 |
| `/qrestart trigger nightly` | arms survival immediately — REQ-051 |
| Crash hub between drain and restart | proxy iterates `fallback-hubs`, drain continues — REQ-013 |

## 8. Rollback

If any test fails irreversibly, drop the plugin jars from `plugins/`,
restart the proxy, and `git revert` the failing commit on the project
branch. The plugin holds no persistent state — config-only.
