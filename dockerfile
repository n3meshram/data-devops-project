FROM apache/spark:4.0.0

USER root

WORKDIR /app

COPY . .

RUN mkdir -p /app/artifacts \
    && chown -R spark:spark /app


RUN pip3 install -r requirements.txt

USER spark

CMD ["python3","run_pipeline.py"]