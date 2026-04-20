# syntax=docker/dockerfile:1.7
#
# Asset Discovery Bot — runtime image
#
# Goals:
#   - Small footprint (slim base, no build toolchain kept in the final image).
#   - Non-root runtime user.
#   - Deterministic deps via pinned requirements.txt.
#   - No inbound ports (no EXPOSE) — the bot only makes outbound HTTPS calls.
#
# Expected resource profile (Requirement 9.1):
#   steady-state ~150 MB RAM, peak ~300 MB during Pandas compute.
#
# Requirements traceability:
#   - 9.1  lean Python container, slim base, no dev tooling at runtime
#   - 9.6  no EXPOSE directive; no inbound ports declared
#
# Note: CMD is intentionally omitted here. Task 13.1 will add the entrypoint
# (`python -m bot.run`) so this task stays focused on the base image shape.

FROM python:3.11-slim AS runtime

# Predictable Python behavior inside containers.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Minimal runtime OS packages:
#   - ca-certificates: required for outbound HTTPS to Wikipedia, Yahoo, FMP, Discord.
# All build toolchain is deliberately absent; runtime deps rely on manylinux
# wheels published to PyPI (psycopg[binary], lxml, pandas, etc.).
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create a dedicated non-root user and group.
RUN groupadd --system --gid 1000 bot \
    && useradd  --system --uid 1000 --gid bot --home-dir /app --shell /usr/sbin/nologin bot

WORKDIR /app

# Install Python dependencies first so this layer caches across code-only changes.
# Installed as root into the system site-packages; the runtime user only needs
# read+execute on them, which is the default.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application source with correct ownership for the non-root user.
COPY --chown=bot:bot bot/ /app/bot/

# Drop privileges before the container runs.
USER bot

# Intentionally NO EXPOSE directive (Requirement 9.6): the bot opens only
# outbound connections and never accepts inbound traffic.

# Default command: run a single Scan_Run and exit. The Synology Task
# Scheduler invokes this image via `docker exec ... python -m bot.run`
# or `docker compose run --rm bot python -m bot.run`; the CMD here is the
# sensible default when the container is started without an explicit
# command (e.g., `docker compose up bot` during development).
#
# Database migrations are NOT run automatically at start-up. Operators
# apply them as a one-shot:
#   docker compose run --rm bot python -m bot.migrations.run_migrations
# See docker-compose.yml and the deployment README (Task 19).
CMD ["python", "-m", "bot.run"]
