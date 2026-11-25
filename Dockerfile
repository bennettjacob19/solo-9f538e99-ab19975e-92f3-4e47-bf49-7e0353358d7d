FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DRILL_DATA_DIR=/drill/config \
    DRILL_STATE_DIR=/drill/state \
    DRILL_HOST=0.0.0.0 \
    DRILL_PORT=8000

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /drill/config /drill/state

EXPOSE 8000

HEALTHCHECK --interval=5s --timeout=3s --retries=12 \
  CMD ["python", "-c", "import urllib.request,sys; r=urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=2); sys.exit(0 if r.status==200 else 1)"]

# 单进程多线程 Flask；发布端是它拉起的独立子进程。
# compose 里 init: true 负责回收僵尸子进程。
CMD ["python", "app.py"]
