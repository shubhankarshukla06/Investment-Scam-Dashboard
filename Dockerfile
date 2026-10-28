FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    chromium \
    chromium-driver \
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-hin \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=10000
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

EXPOSE 10000

CMD ["sh", "-c", "exec gunicorn -b 0.0.0.0:${PORT:-10000} --workers 1 --timeout 600 --graceful-timeout 30 app:app"]
