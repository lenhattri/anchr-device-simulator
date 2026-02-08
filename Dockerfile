FROM python:3.9-slim

WORKDIR /app

RUN pip install paho-mqtt aiohttp pyyaml

COPY simulator.py .
# Config is optional and provided via Env Vars

CMD ["python", "simulator.py"]
