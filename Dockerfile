FROM ubuntu:24.04 AS chronos-build

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake pkg-config libpqxx-dev libhiredis-dev librdkafka-dev \
    ca-certificates curl unzip libssl-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY CMakeLists.txt ./
COPY include ./include
COPY src ./src
COPY tests ./tests
COPY sql ./sql
COPY docs ./docs

RUN cmake -S . -B /build -DCMAKE_BUILD_TYPE=Release \
    -DCHRONOS_ENABLE_POSTGRES=ON \
    -DCHRONOS_ENABLE_REDIS=ON \
    -DCHRONOS_ENABLE_KAFKA=ON \
    -DBUILD_TESTING=OFF && \
    cmake --build /build --parallel 2

FROM ubuntu:24.04 AS chronos-runtime

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpqxx-7.8 libhiredis1.1.0 librdkafka1 ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --system --create-home --uid 10001 chronos

COPY --from=chronos-build /build/chronos /usr/local/bin/chronos
COPY --from=chronos-build /build/chronos_api_gateway /usr/local/bin/chronos_api_gateway
COPY --from=chronos-build /build/chronos_postgres_demo /usr/local/bin/chronos_postgres_demo
COPY --from=chronos-build /build/chronos_scheduler_service /usr/local/bin/chronos_scheduler_service
COPY --from=chronos-build /build/chronos_worker_manager_service /usr/local/bin/chronos_worker_manager_service
COPY --from=chronos-build /build/chronos_worker_agent /usr/local/bin/chronos_worker_agent
COPY sql /app/sql

USER chronos
WORKDIR /app

ENTRYPOINT ["chronos_api_gateway"]
