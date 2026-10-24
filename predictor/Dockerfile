FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY predictor/requirements.txt /app/predictor/requirements.txt
RUN pip install --no-cache-dir -r /app/predictor/requirements.txt && \
    mkdir -p /app/predictor/artifacts && \
    chown -R 65532:65532 /app

COPY --chown=65532:65532 predictor/ /app/predictor/

USER 65532:65532

# Pre-train deterministic CPU model during build and verify holdout metrics
RUN python3 -m predictor.train --artifact-dir /app/predictor/artifacts

EXPOSE 8081

ENTRYPOINT ["python3", "-m", "predictor.app", "--host", "0.0.0.0", "--port", "8081", "--artifact-dir", "/app/predictor/artifacts"]
