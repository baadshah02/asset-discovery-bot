# asset-discovery-bot

Containerised research tool that scans the S&P 500 once a day, applies a
sequential 4-layer value-and-quality filter inspired by George & Hwang
(2004), Fama-French, and Asness's Quality Minus Junk, and posts
high-conviction candidates to Discord for **human review**.

> **Scope.** v1 is an *alert and research tool*, not a capital deployment
> system. Alerts are paper-trading signals; compare realised performance
> against a factor-tilted ETF (AVUV, IWD, RPV) for at least 6–12 months
> before considering any live deployment. See
> [`.kiro/specs/asset-discovery-bot/design.md`][design] (Known Limitations
> and v2 Roadmap) for the full caveats.

[design]: .kiro/specs/asset-discovery-bot/design.md

---

## Table of contents

1. [Prerequisites](#prerequisites)
2. [One-time host setup](#one-time-host-setup)
3. [First-run sequence](#first-run-sequence)
4. [Scheduling daily scans](#scheduling-daily-scans)
5. [Configuration](#configuration)
6. [Operations](#operations)
7. [Deployment variants](#deployment-variants)
8. [Rollback](#rollback)
9. [Architecture at a glance](#architecture-at-a-glance)

---

## Prerequisites

Any Docker-capable host works. The canonical deployment target is a
**Synology DS220+ (DSM 7.2+)** with Container Manager; the
instructions below assume that environment and note deviations for
generic Linux hosts.

- Docker Engine ≥ 20.10, Docker Compose v2.
- ≥ 1 GB free RAM (Postgres is capped at 512 MB; the bot container peaks
  at ~300 MB during Pandas computation).
- Outbound HTTPS egress to:
  - `en.wikipedia.org` (constituent scrape)
  - `query1.finance.yahoo.com` (OHLC via `yfinance`)
  - `financialmodelingprep.com` (fundamentals + press releases)
  - `discord.com` (webhook posts)
- A Discord server and channel plus a [Webhook URL][discord-hooks].
- A [Financial Modeling Prep][fmp] free-tier API key (250 calls/day).

[discord-hooks]: https://support.discord.com/hc/en-us/articles/228383668
[fmp]: https://financialmodelingprep.com/developer/docs

---

## One-time host setup

### Directory layout on a Synology DS220+

All bot state lives under a single top-level directory so it is easy to
back up and hard to accidentally trample other services:

```
/volume1/docker/asset-discovery-bot/
├── data/
│   ├── pg/         # Postgres datadir    (pgdata volume)
│   └── logs/       # Rotating JSON logs  (logs volume)
├── config/         # config.yaml lives here  (config volume)
└── secrets/        # fmp_api_key.txt, db_password.txt, discord_webhook_url.txt
```

Create them from a shell (Synology: SSH in as an admin user):

```bash
sudo mkdir -p /volume1/docker/asset-discovery-bot/{data/pg,data/logs,config,secrets}
sudo chmod 750 /volume1/docker/asset-discovery-bot/secrets
```

### Seed the three Docker secrets

The compose file expects the secrets as three plain-text files under
`./secrets/` (relative to the compose project root). Put **one value
per file, no trailing newline**.

```bash
cd /volume1/docker/asset-discovery-bot

# 1) Postgres superuser password — pick a strong random value.
openssl rand -base64 32 | tr -d '\n' > secrets/db_password.txt

# 2) FMP free-tier API key (copy from your FMP dashboard).
printf '%s' 'YOUR_FMP_API_KEY' > secrets/fmp_api_key.txt

# 3) Discord channel webhook URL.
printf '%s' 'https://discord.com/api/webhooks/XXXX/YYYY' > secrets/discord_webhook_url.txt

chmod 600 secrets/*.txt
```

**Never** commit these files. The repository `.gitignore` already excludes
`/volume1/`.

### Seed `config.yaml`

Copy the example config into the mounted volume and edit thresholds to
taste:

```bash
cp config/config.example.yaml config/config.yaml
# Edit config/config.yaml if you want to tune thresholds.
```

The full schema is validated by
[`bot/config.py`](bot/config.py#L77) at startup; any invalid value fails
the run non-zero *before* any network, database, or Discord I/O
(Requirement 6.4 / 11.12).

### Clone / copy the repo to the host

The compose file builds the image from the local `Dockerfile`, so the
source tree must live on the host:

```bash
cd /volume1/docker/asset-discovery-bot
git clone <your-remote-here> app
cd app
```

From this point `docker compose` commands are run from
`/volume1/docker/asset-discovery-bot/app/`.

---

## First-run sequence

```bash
# 1. Build the bot image.
docker compose build

# 2. Start Postgres and wait for its healthcheck to report healthy.
docker compose up -d db
docker compose ps     # STATUS column should show (healthy) within ~30s

# 3. Apply database migrations. Idempotent — safe to re-run.
docker compose run --rm bot python -m bot.migrations.run_migrations

# 4. Run one scan manually to confirm the full pipeline end-to-end.
docker compose run --rm bot python -m bot.run
```

The first run scrapes Wikipedia, downloads a year of OHLC for ~500
tickers, and populates `asset_universe`. It typically takes 3–5 minutes.
Successful completion means:

- Exit code `0`.
- One INFO log line reporting the triage cardinalities (`universe=500
  -> L1=... -> L2=...`) and FMP call count.
- Zero or more rows in `daily_scans` (most days produce none).
- Zero or more Discord embeds posted to the channel.

---

## Scheduling daily scans

### Synology Task Scheduler

1. DSM → Control Panel → Task Scheduler → Create → Scheduled Task →
   User-defined script.
2. **General**
   - Task: `asset-discovery-bot`
   - User: `root` (needed to execute `docker`).
3. **Schedule**
   - Daily, run at a time ≥ 30 minutes after US market close (e.g.,
     21:30 America/New_York ≈ 02:30 Synology local if the NAS is on
     Pacific Time; adjust for your timezone).
4. **Task Settings → Run command**:
   ```bash
   cd /volume1/docker/asset-discovery-bot/app \
     && /usr/local/bin/docker compose run --rm bot python -m bot.run
   ```
5. Enable "Notify me by email" on non-zero exit so delivery failures
   surface.

### Generic Linux host (systemd)

`~/.config/systemd/user/asset-discovery-bot.service`:

```ini
[Unit]
Description=Asset Discovery Bot — daily scan
Wants=asset-discovery-bot.timer

[Service]
Type=oneshot
WorkingDirectory=/opt/asset-discovery-bot
ExecStart=/usr/bin/docker compose run --rm bot python -m bot.run
```

`~/.config/systemd/user/asset-discovery-bot.timer`:

```ini
[Unit]
Description=Daily asset-discovery-bot scan

[Timer]
OnCalendar=Mon..Fri 21:30 America/New_York
Persistent=true

[Install]
WantedBy=timers.target
```

`systemctl --user enable --now asset-discovery-bot.timer`.

### Cron (minimal)

```cron
30 21 * * 1-5 cd /opt/asset-discovery-bot && docker compose run --rm bot python -m bot.run >> /var/log/asset-discovery-bot.log 2>&1
```

### Kubernetes (CronJob)

A CronJob variant is out of scope for the v1 on-host deployment but
follows the same pattern: build the image, mount the three Docker
secrets as Kubernetes secrets, and schedule `python -m bot.run` once a
day via `spec.schedule: "30 2 * * 1-5"`.

---

## Configuration

All tunables live in `config/config.yaml`. The file is re-read on every
invocation. The full schema with validator ranges is documented in
`config/config.example.yaml` and enforced by `bot/config.py`.

Three ways to override a value, highest precedence first:

1. **Environment variable** `ADB_<SECTION>__<FIELD>` (double-underscore
   delimiter). Example: `ADB_LAYER4__FCF_YIELD_MIN=0.04`.
2. **`config.yaml`** on disk.
3. **Pydantic default** in `bot/config.py`.

Invalid values (negative thresholds, RSI outside `[0, 100]`,
`pct_above_low_max <= pct_above_low_min`, staleness outside `[1, 90]`,
etc.) cause the bot to exit non-zero **before** any I/O.

---

## Operations

### Logs

Structured JSON, one object per line, written to the `logs` volume at
`/var/log/asset-discovery-bot/asset-discovery-bot.log` inside the
container (`/volume1/docker/asset-discovery-bot/data/logs/` on the
host). Rotated at `logging.max_file_size_mb` MB with
`logging.backup_count` archives. Every line carries a `run_id`
(UUID4 hex) that ties together all records from one Scan_Run.

Tail the live output:

```bash
tail -F /volume1/docker/asset-discovery-bot/data/logs/asset-discovery-bot.log | jq .
```

Filter to one run:

```bash
jq -c 'select(.run_id == "7f2a…")' /volume1/docker/asset-discovery-bot/data/logs/asset-discovery-bot.log
```

### Exit codes

| Code | Meaning                                        | Action                           |
|-----:|------------------------------------------------|----------------------------------|
|   0  | Success (zero or more alerts delivered)        | —                                |
|   1  | Unexpected / unhandled error                   | Check logs                       |
|   2  | Config load or validation failed               | Inspect `config.yaml` / secrets  |
|   3  | Wikipedia scrape failed or bounds mismatch     | Check network; retry next day    |
|   4  | Postgres unreachable or operational error      | Check `db` container             |
|   5  | Discord webhook retry budget exhausted         | Check webhook URL / Discord UI   |

The scheduler should alert on any non-zero code.

### Secret rotation

```bash
# Rotate Discord webhook
printf '%s' 'https://discord.com/api/webhooks/NEW…' > secrets/discord_webhook_url.txt
docker compose run --rm bot python -m bot.run   # next invocation picks it up

# Rotate FMP API key — same pattern, write new key then next run picks it up.

# Rotate Postgres password — more involved; see Rollback section.
```

Secrets files are re-read on every container start because they are
mounted as Docker secrets at `/run/secrets/<name>`. No image rebuild
needed.

### Database maintenance

`daily_scans` grows by at most the number of Discord alerts per day
(typically 0–5). At that rate the table is under 2000 rows per year;
no routine maintenance is needed in v1.

Ad-hoc queries:

```bash
docker compose exec db psql -U adb -d asset_discovery_bot -c "
  SELECT scan_date, ticker, fcf_yield, pe_ratio, pe_5y_avg
  FROM daily_scans
  WHERE scan_date >= CURRENT_DATE - 30
  ORDER BY scan_date DESC, ticker;
"
```

---

## Deployment variants

The compose file is deliberately portable. To deploy somewhere other
than a Synology DS220+:

- Point `secrets:` `file:` paths at a secret store of your choice
  (Docker Swarm secrets, Kubernetes secrets mounted as files, etc.).
- Point the `pgdata`, `logs`, and `config` named volumes at bind mounts
  under whatever directory layout fits the host.
- The image itself has no host-specific logic.

---

## Rollback

The bot is a batch process — rollback is just restarting it on the
previous image tag. Procedure:

```bash
cd /volume1/docker/asset-discovery-bot/app
git fetch && git checkout <previous-tag-or-sha>
docker compose build
# Next scheduled run uses the rolled-back image.
```

To roll back the schema, restore the Postgres data directory from a
Synology Hyper Backup snapshot of `/volume1/docker/asset-discovery-bot/data/pg`
and restart the `db` container. **Never run `DROP TABLE` against a
production database by hand.**

---

## Architecture at a glance

```
Synology DS220+
├─ Task Scheduler ──(docker compose run)──▶ bot container
│                                             │
│                                             │  reads
│                                             ▼
│                                        /run/secrets/*  +  /app/config/config.yaml
│                                             │
│  ┌──────────────────────────────────────────┴─────────────────────────────┐
│  │   bot.run → sync_universe (Wikipedia)                                  │
│  │          → download_price_history (yfinance)                           │
│  │          → Layer 1, Layer 2                                            │
│  │          → get_fundamentals ONLY for L2 survivors (FMP, cache-gated)   │
│  │          → Layer 3, Layer 4                                            │
│  │          → insert_scan (Postgres) → send_high_conviction (Discord)     │
│  └────────────────────────────────────────────────────────────────────────┘
└─ Postgres 15-alpine (db container, 512 MB cap, internal network only)
```

Full architecture, filter semantics, and correctness properties live in
[`.kiro/specs/asset-discovery-bot/design.md`][design] and
[`.kiro/specs/asset-discovery-bot/requirements.md`][reqs].

[reqs]: .kiro/specs/asset-discovery-bot/requirements.md
