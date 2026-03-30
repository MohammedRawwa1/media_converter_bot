FROM python:3.12-slim
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . /app
RUN pip install --upgrade pip && pip install -r requirements.txt
ENV FFMPEG_PATH=/usr/bin/ffmpeg FFPROBE_PATH=/usr/bin/ffprobe PORT=10000
EXPOSE 10000
CMD ["sh", "-c", "python scripts/check_env.py && uvicorn main:app --host 0.0.0.0 --port $PORT"]
FROM python:3.12-slim
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . /app
RUN pip install --upgrade pip && pip install -r requirements.txt
ENV FFMPEG_PATH=/usr/bin/ffmpeg FFPROBE_PATH=/usr/bin/ffprobe PORT=10000
EXPOSE 10000
CMD ["sh", "-c", "if [ -f ./scripts/check_env.py ]; then python ./scripts/check_env.py || exit 1; fi; uvicorn main:app --host 0.0.0.0 --port $PORT"]
