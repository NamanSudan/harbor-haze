# One image for every pipeline process: producers, Spark batch jobs and Spark streaming jobs.
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-17
COPY --from=ghcr.io/astral-sh/uv:0.7.19 /uv /usr/local/bin/uv

ENV JAVA_HOME=/usr/lib/jvm/java-17 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    HARBOR_DATA_DIR=/app/data
WORKDIR /app

# dependencies first (cached layer), then the package itself
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev

# pre-fetch the Spark Kafka connector so streaming jobs start offline
RUN python -c "from harbor_haze.spark import get_spark; get_spark('warmup', kafka=True).stop()" || true
