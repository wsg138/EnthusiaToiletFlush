# Network restart operations

Velocity is the authoritative scheduler. Configure all recurring restart plans
in the Velocity plugin's `config.yml`; do not configure a second schedule in a
backend or another restart plugin.

## Restart types

- `server` uses the existing ToiletFlush backend drain, hub transfer, Paper
  companion shutdown, CheckHacks gate, and rank-weighted rejoin queue. Only
  players on that backend are affected.
- `proxy` restarts only Velocity. All players are fully disconnected. Backend
  servers remain online.
- `network` restarts the non-hub backends and hubs listed in
  `network-restart.full-network.members`, then sends the proxy restart action
  last. This is not a Pterodactyl-account-wide action.

## Pterodactyl setup

1. Install the Velocity jar on the proxy and the Paper companion jar on every
   backend. Do not install the Velocity jar on backends.
2. In Pterodactyl, create a Client API key with access only to the configured
   proxy and Minecraft backend servers.
3. Provide it to the Velocity process as `PTERODACTYL_API_KEY`. Leave
   `api-key: ${PTERODACTYL_API_KEY}` in the config; never paste the key into
   the file or a command.
4. Use the panel origin as `panel-url`, such as `https://panel.example.com`.
   Do not include a `/server/<id>` page URL.
5. Use each Pterodactyl server's identifier from its panel URL as
   `proxy-server-id` or a `network-restart.servers` value. The identifiers are
   allow-listed by the config, so commands cannot select arbitrary panel
   servers.
6. Set `enabled: true` while keeping `executor: DRY_RUN`, then run
   `/qrestart reload`.

The executor uses Pterodactyl's Client API with HTTPS, bounded timeouts, no
redirects, and no retry of an uncertain restart power action. A rejected or
failed request leaves Velocity running, clears any active maintenance lock,
and records the failed plan. There is intentionally no local-shutdown fallback
presented as a substitute for an externally managed proxy restart.

## Safe deployment and dry run

1. Fill in Velocity server names, Pterodactyl identifiers, and the explicit
   full-network member list.
2. Set `enabled: true` and `executor: DRY_RUN`; reload the plugin.
3. Trigger the configured proxy or network schedule and verify countdown
   messages, plan creation, status commands, and persistence.
4. Confirm that DRY_RUN does not transfer or disconnect players, enable
   maintenance mode, contact Pterodactyl, or issue power actions.
5. Verify `/nextrestart` and `/restartschedule` show only public plans.
6. Set `executor: PTERODACTYL`, reload, and first test one non-critical backend.
7. Test a short proxy restart during a maintenance window, then test the full
   network sequence only after the proxy and backend identifiers are verified.

`DRY_RUN` is intentionally non-disruptive. It validates scheduling and
countdown behavior, records a clearly marked dry-run result, and performs no
player or server lifecycle actions. Dry-run records are excluded from
`/lastrestart` history.

## Recurring schedules and migration

The default sample schedules SMP at midnight Monday through Saturday and a
full-network restart at midnight Sunday in `America/Indiana/Indianapolis`.
Each has a two-hour warning window, so warnings begin at 10:00 PM.

Automatic schedules run only when `network-restart.enabled` is `true`. Real
proxy and full-network restarts additionally require `executor: PTERODACTYL`,
a valid `PTERODACTYL_API_KEY`, and correct panel identifiers for every listed
member and the proxy.

To migrate from a previous restart plugin, disable its automatic schedules and
remove its Velocity restart commands before enabling ToiletFlush schedules.
Keep the ToiletFlush Paper companion: normal backend drain/rejoin behaviour is
unchanged. Do not leave a duplicate Skript or another proxy scheduler active.

## Recovery and troubleshooting

Plans are persisted in `network-restarts.state` using atomic replacement.
Future plans resume after a proxy restart. A future server plan that was
already counting down is safely re-armed with only its remaining time, so past
warning marks are not replayed. Overdue plans are marked missed. Plans
interrupted during preflight, transfer, or dispatch are marked
`NEEDS_REVIEW` and are never replayed automatically, preventing restart loops.

Countdown message templates and sound definitions are resolved at each mark,
so a successful `/qrestart reload` affects active and future countdowns. A
failed reload leaves the last-known-good configuration active.

If preflight fails, no power actions are sent. Executor and target mappings are
snapshotted when execution begins, so `/qrestart reload` cannot switch an
in-flight plan between DRY_RUN and PTERODACTYL or redirect later actions. If a
non-hub backend restart is rejected after preflight, the sequence stops before
hubs are disconnected or Velocity is restarted. Any earlier accepted action is
recorded and is never retried automatically. After an accepted proxy restart
request, the old proxy keeps its login maintenance gate until it exits; if it
does not exit, the gate expires after `maintenance-failure-expiry-seconds`.

Check the proxy log for the plan identifier and Pterodactyl HTTP status. API
keys are not written to logs or restart-state files.
