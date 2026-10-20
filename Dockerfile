# Pipeline image: Python 3.12 + Java 17 (Debian bookworm's default JRE) + PySpark.
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=UTC

WORKDIR /app

# Dependencies first (cached layer), read straight from pyproject.toml so versions are
# defined in exactly one place.
COPY pyproject.toml ./
RUN python -c "import tomllib; print('\n'.join(tomllib.load(open('pyproject.toml', 'rb'))['project']['dependencies']))" > /tmp/requirements.txt \
    && pip install -r /tmp/requirements.txt

COPY README.md LICENSE ./
COPY src ./src
COPY sql ./sql
COPY scripts ./scripts
# Editable install keeps sql/ resolvable next to src/ for the loader.
RUN pip install --no-deps -e . \
    && useradd --uid 1000 --create-home app \
    && mkdir -p /app/data && chown app:app /app/data

USER app
CMD ["bash", "scripts/run_demo.sh"]
