FROM python:3.12-slim

WORKDIR /app

ARG APP_REVISION
RUN case "$APP_REVISION" in *[!0-9a-f]*|'') exit 1 ;; esac; \
    test "${#APP_REVISION}" -eq 40; \
    printf '%s\n' "$APP_REVISION" > /app/REVISION
LABEL org.opencontainers.image.revision=$APP_REVISION

RUN groupadd --system sniper && useradd --system --gid sniper --create-home --home-dir /app sniper

RUN pip install --no-cache-dir uv==0.12.5

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY README.md ./
COPY src ./src
COPY configs ./configs
COPY migrations ./migrations
COPY scripts ./scripts
COPY alembic.ini ./alembic.ini
COPY .env.example ./.env.example

RUN uv sync --frozen --no-dev
RUN mkdir -p /app/data /app/tmp && chown -R sniper:sniper /app/data /app/tmp

ENV PATH="/app/.venv/bin:$PATH"
USER sniper
EXPOSE 8080

CMD ["sniper-bot", "--config", "configs/default.yaml", "--host", "0.0.0.0", "--port", "8080"]
