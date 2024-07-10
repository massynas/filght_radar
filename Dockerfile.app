FROM python:3.9-slim

WORKDIR /app
COPY app.py /app

RUN pip install dash dash-core-components dash-html-components plotly pyspark

CMD ["python", "app.py"]
