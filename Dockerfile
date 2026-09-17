# SentinelView AI Ops Dashboard -- built from the repo root so the container
# can see both dashboard/ and the demos/ agent scripts it shells out to.
FROM python:3.12-slim

WORKDIR /app

COPY dashboard/backend/requirements.txt dashboard/backend/requirements.txt
RUN pip install --no-cache-dir -r dashboard/backend/requirements.txt

COPY demos demos
COPY dashboard/backend dashboard/backend
COPY dashboard/frontend dashboard/frontend

ENV PORT=8080
EXPOSE 8080
WORKDIR /app/dashboard/backend
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT}
