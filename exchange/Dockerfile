FROM python:3

RUN pip install Flask

WORKDIR /app

COPY . .

EXPOSE 5000
EXPOSE 7432

CMD ["python3", "exchange.py"]
