# Requirements

## Scheduling & countdown
- REQ-001: When an operator runs `/schedrestart <minutes> [server]`, the system shall arm a restart for the named server (or the operator's current server if omitted) and begin a countdown of the requested minutes.
- REQ-002: When a configured cron schedule fires, the system shall arm a restart for the schedule's target server and begin a countdown of the schedule's `warn-minutes` value.
- REQ-003: While a restart is armed, the system shall broadcast a configured countdown message to players currently on the target server at every configured mark-second value.
- REQ-004: While a restart is armed, the system shall play a configured Adventure sound to players currently on the target server at every mark-second value.
- REQ-005: If an operator runs `/schedrestart cancel [server]` while a restart is armed, then the system shall transition the coordinator for that server back to idle and broadcast a cancellation message.
- REQ-006: Where the configured sound volume exceeds 0.8, the system shall log a startup warning identifying the offending key.

## Drain
- REQ-010: When the visible countdown reaches T-0, the system shall begin transferring players from the target server to the configured hub in batches of `batch-size`, spaced by `batch-interval-ticks`; it shall not move players before T-0.
- REQ-011: While a drain is in progress, the system shall order outgoing players according to the configured `drain-order` setting using rank weights resolved from the rank ladder.
- REQ-012: The system shall send the backend restart signal only after the target is empty; if `force-drain-timeout-seconds` elapses, it shall first issue configured disconnects to remaining players and then send the immediate restart as a safety fallback.
- REQ-013: If the configured hub server is unreachable or unregistered, then the system shall iterate `fallback-hubs` in order and use the first reachable entry.
- REQ-014: Where a player holds permission `queuerestart.bypass.drain`, the system shall not include that player in the drain cohort.

## Restart execution
- REQ-020: When the T-0 drain leaves the target empty (or after remaining players receive the force-timeout disconnect), the system shall publish an immediate `RestartNow` through both the plugin-message and SLP poll-back delivery paths.
- REQ-021: When the companion receives a `RestartNow` message, the companion shall execute the operator-configured restart action (`SHUTDOWN`, `COMMAND`, or `EXIT_CODE`).
- REQ-022: After the T-0 drain completes, the system shall publish an immediate pending arm into a backend-addressable SLP poll-back response so a companion with no online players can discover and execute the restart. When the companion polls the proxy and the response carries a `QR_ARM` sample entry, the companion shall execute the supplied action.

## Rejoin queue
- REQ-030: When a restart is armed, the system shall snapshot the current player list of the target server as the rejoin cohort.
- REQ-031: While the target server is offline after a restart, the system shall poll its liveness via `RegisteredServer.ping()` at the configured interval.
- REQ-032: When the target server's ping succeeds for the first time after a restart, the system shall enqueue every cohort member that is currently online into VelocityCTD's queue for that server.
- REQ-033: While enqueuing the rejoin cohort, the system shall set each player's queue weight from the configured rank ladder using the highest-weighted matching permission.
- REQ-034: If a cohort member is offline when enqueuing begins, then the system shall not enqueue that player.

## CheckHacks gate
- REQ-040: While a cohort member is on the hub awaiting rejoin, the system shall hold them in the queue until a `CheckHacksResult` plugin message reports them clean.
- REQ-041: When a `CheckHacksResult` message reports `outcome=DETECTED` for a cohort member, the system shall remove that player from the rejoin queue and broadcast no further rejoin messages for them.
- REQ-042: If `check-gate-timeout-seconds` elapses for a cohort member without a `CheckHacksResult` message, then the system shall release that player to the queue when `release-on-timeout` is true, otherwise drop them from the cohort.
- REQ-043: Where a player holds permission `queuerestart.bypass.checkhacks`, the system shall release that player to the queue without waiting for any `CheckHacksResult`.

## Access denial messages
- REQ-024: While a backend is draining, restarting, or offline for a managed restart, the proxy shall deny new server-switch attempts with a configurable message.
- REQ-025: When Paper rejects a backend connection because the backend is whitelisted, the proxy shall preserve the player's existing proxy session where possible and show a configurable whitelist message.

## Configuration & ops
- REQ-050: When `/qrestart reload` is invoked by an operator, the system shall reload `config.yml` and reschedule cron entries without aborting any in-flight countdown.
- REQ-051: When `/qrestart trigger <scheduleName>` is invoked by an operator, the system shall arm the named schedule's restart immediately as if its cron had fired.
- REQ-052: While the proxy is running, the system shall expose `/schedrestart status` showing every armed coordinator and its current state.

## Safety
- REQ-060: If an operator targets the configured hub server with `/schedrestart`, then the system shall refuse the request and log an error.
- REQ-061: If two restart requests arrive for the same target server while one is already armed, then the system shall reject the second request and inform the operator.
- REQ-062: If the companion plugin is missing on the target server when a restart is requested, then the system shall refuse to arm the restart and log an error.

## Security hardening
- REQ-090: While the system processes inbound traffic on either `qrestart:v1` plugin-message frames or `QR_POLL:` SLP handshakes, the system shall reject any frame whose source cannot be authenticated to a registered backend server, drop client-origin plugin-message frames on the proxy before forwarding, refuse non-`SHUTDOWN` arm modes on the SLP poll-back path, gate every CheckHacks verdict on the player being currently present on the source backend, restrict the schedule-announce SLP sample to loopback/RFC1918/link-local peers unless explicitly opened, enforce a 64-character `[A-Za-z0-9_.-]` constraint on parsed server-ids, and back every cross-thread shared collection with a thread-safe implementation.
  - Note: phase 1 closes the immediate exploitability gaps (findings #1, #3, #4, #5, #6, B, C, D, #8, #9, #10, I from the May 2026 peer audit). Phase 2 will introduce a shared secret + HMAC over both channels to close the remaining authenticity gaps (findings A, B, E, G).
