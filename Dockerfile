FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock ./
COPY README.md ./
COPY src/ ./src/

RUN uv pip install --system -e .

RUN playwright install --with-deps chromium

ENV DAGSTER_HOME=/app/.dagster

RUN mkdir -p /app/.dagster

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3001", "-f", "src/definitions.py"]



