FROM python:3.13-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.lock ./requirements.lock
RUN python -m pip install --requirement requirements.lock
COPY app ./app
RUN useradd --create-home --uid 10001 gateway
USER gateway
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/health/live' % os.getenv('PORT','8080'), timeout=2)"
CMD ["sh", "-c", "exec uvicorn app.main:create_app --factory --host 0.0.0.0 --port ${PORT:-8080} --ws-max-size 16384 --workers 1"]
