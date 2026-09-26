# Python side of the ABI stack (Phase 5 deploy): ONE image, several roles, chosen
# by the compose `command:` - the model sidecar (ml/serve.py), the NL-BI agent
# (ml/agent/server.py), the drift monitor and the governed retrain worker
# (ml/monitor/*), and the one-shot bootstrap (download -> bronze -> dbt -> train).
# Dependencies come from uv.lock (frozen), including the `ml` group.
FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.12 /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1 UV_PYTHON_DOWNLOADS=never UV_NO_CACHE=1 \
    PYTHONUNBUFFERED=1 PATH="/app/.venv/bin:$PATH"
WORKDIR /app
# libgomp: OpenMP runtime needed by the LightGBM/XGBoost wheels.
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 abi && chown abi /app

# Everything below is created as the runtime user: no recursive chown layer
# re-copies the virtualenv (that doubled the image), and uv keeps no cache.
USER abi
COPY --chown=abi pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --group ml --no-install-project

COPY --chown=abi ml/ ml/
COPY --chown=abi loader/ loader/
COPY --chown=abi scripts/ scripts/
COPY --chown=abi dbt/ dbt/
COPY --chown=abi deploy/bootstrap.sh deploy/bootstrap.sh
# The shared contracts the Python side reads from the Go tree (single-sourced).
COPY --chown=abi api/internal/predict/schema.sql api/internal/predict/schema.sql
COPY --chown=abi api/internal/actions/schema.sql api/internal/actions/schema.sql
COPY --chown=abi api/internal/scorewriter/fraud_feature_spec.json api/internal/scorewriter/fraud_feature_spec.json
COPY --chown=abi api/internal/scorewriter/bot_feature_spec.json api/internal/scorewriter/bot_feature_spec.json

# Trained artifacts are NOT baked in: they are a build product of training on the
# target machine, mounted at /app/artifacts (same policy as the repo's .gitignore).
RUN mkdir -p /app/artifacts
CMD ["python", "ml/serve.py"]
