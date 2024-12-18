FROM gcc:14.2

RUN apt-get update && apt-get install -y g++ cmake nlohmann-json3-dev

WORKDIR /app

COPY . .

RUN cmake .
RUN make

EXPOSE 7432

CMD ["./bin/pr4"]
