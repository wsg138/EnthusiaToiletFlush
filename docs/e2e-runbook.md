# End-to-end manual verification runbook

Covers implementation.md §1, §5 — the full state cycle of one armed
restart, exercised against a dev proxy with two backends + a hub.

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
- **Auto-restart on clean exit must be enabled for every backend.** The
  companion calls `Bukkit.shutdown()` to restart — the JVM exits cleanly
  and the supervisor relaunches it. Without auto-restart the server just
  stays down after the first scheduled restart.
  - **Bloom / Pterodactyl panels:** flip *Misc Options → Crash Detection*
    ON. The panel treats a normal Bukkit exit as a crash and relaunches.
  - **Local / bare-metal:** wrap your launch command in a loop (see
    `test_net/lobby2/launch-windows.bat` for the Windows pattern).
  - **systemd:** `Restart=always` on the unit.

## 1. Boot smoke test

1. Start hub, both backends, then the proxy.
2. Watch the proxy log for:
   - `[queue-restart] config loaded from plugins/queue-restart/config.yml`
   - no `sound volume` warnings (default suite stays under 0.8).
   - `recurring schedules are configured on Velocity` — automatic plans are
     created from `automatic-schedules` in `config.yml` when their warning
     windows begin.
3. Watch each backend log for:
   - `[queue-restart-companion] enabled` and either
     `CheckHacks integration: present` or `CheckHacks integration: absent`.

## 2. Armed → countdown → drain → restart

1. As op: `/server survival`, then `/schedrestart 1`.
2. Expect chat: `Server <yellow>survival</yellow> restarts in <yellow>1m</yellow>`
   plus a sound at every configured mark (60s, 30s, 10..1s, T-0).
3. Before T-0, confirm nobody is moved from the backend.
4. At T-0, players begin transferring to `lobby` in batches of 10
   (default), spaced 40 ticks apart.
5. Confirm the proxy waits until `survival` reports zero connected players,
   then publishes an immediate restart arm. The companion shuts down the
   backend on receipt (the empty-server SLP fallback may add up to one
   `arm-poll-seconds` interval).
6. Proxy state advances to `RESTART_SENT` only after the immediate arm is
   published.

## 3. Server down → ping ok → rejoin

1. Proxy polls `survival.ping()` every `ping-poll-seconds` (default 3s).
   Log shows `coordinator(survival): SERVER_DOWN, polling…`.
2. When the backend is back: `coordinator(survival): REJOIN_RELEASE`.
3. Every cohort member that is still online gets enqueued for `survival`
   with their rank weight (`queue` debug logs from VelocityCTD show this).
4. Owner (weight 1000) gets in before default (weight 0).
5. Coordinator returns to IDLE.

## 4. CheckHacks gate (optional, requires CheckHacks-fork PR)

1. With CheckHacks-fork installed, repeat §2 against `survival`.
2. While players sit on `lobby` waiting to be released, run a CheckHacks
   sign check against one of them.
3. CLEAN result → released into queue.
4. DETECTED result → removed from queue, broadcast suppressed.
5. Wait `check-gate-timeout-seconds` (default 60s) without firing the
   event → released (because `release-on-timeout: true`).

## 5. Negative / safety paths

| Action | Expected |
|---|---|
| `/schedrestart 5 lobby` | rejected — REQ-060 |
| Run `/schedrestart 5` on `survival` twice in a row | second rejected — REQ-061 |
| Stop the companion on `survival`, then `/schedrestart 5` | rejected — REQ-062 |
| `/schedrestart cancel` while ARMED | broadcast `restart cancelled` — REQ-005 |
| `/schedrestart cancel` while IDLE | rejected |
| Player with `queuerestart.bypass.drain` on `survival` during drain | not transferred; receives the configured safety disconnect before restart — REQ-012/014 |
| Player with `queuerestart.bypass.checkhacks` after restart | released without waiting — REQ-043 |
| `/qrestart reload` mid-countdown | reload OK, countdown unaffected — REQ-050 |
| `/qrestart trigger nightly` | arms survival immediately — REQ-051 |
| Crash hub between drain and restart | proxy iterates `fallback-hubs`, drain continues — REQ-013 |

## 6. Rollback

If any test fails irreversibly, drop the plugin jars from `plugins/`,
restart the proxy, and `git revert` the failing commit on the project
branch. The plugin holds no persistent state — config-only.


## Whitelist / restart denial messages

1. Start a managed backend countdown and wait until T-0 begins draining.
2. From the hub, use the normal NPC or GUI to join that backend. Confirm the player remains on the hub and receives `access-messages.backend-restarting`.
3. After the managed restart completes, whitelist the backend and repeat the NPC/GUI join. Confirm the player remains connected to the proxy and receives `access-messages.backend-whitelisted`.
4. Test a direct initial connection with no usable hub and confirm the configured message is used as the disconnect reason.
5. Confirm an unrelated backend kick reason is not replaced.
