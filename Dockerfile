FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
# Install OCR + PDF-to-image tools + PostgreSQL client libs (for psycopg2)
RUN apt-get update   && apt-get install -y --no-install-recommends tesseract-ocr tesseract-ocr-eng poppler-utils libpq-dev gcc   && rm -rf /var/lib/apt/lists/*
# Python deps (pinned versions from pip freeze)
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
EXPOSE 8000
# Run as non-root user
RUN adduser --disabled-password --no-create-home --uid 1000 appuser
RUN rm -f /app/.env
USER appuser

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
