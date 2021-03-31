FROM python:3.6-slim
WORKDIR /app
RUN pip install awswrangler==2.6.0
ENV ENV=TESTING
COPY . .