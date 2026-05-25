FROM python:3.12.7-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
ARG PIP_INSTALL_FLAGS="--trusted-host pypi.org --trusted-host files.pythonhosted.org"
RUN pip install --no-cache-dir ${PIP_INSTALL_FLAGS} -r requirements.txt

COPY app ./app

RUN useradd --system --uid 1000 --no-create-home appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8001
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
