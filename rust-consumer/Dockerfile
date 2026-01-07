# Imagen base Rust
FROM rust:1.83-slim

# Crear directorio de la app
WORKDIR /app

# Instalar dependencias del sistema para rdkafka
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        cmake \
        build-essential \
        pkg-config \
        libssl-dev \
        libsasl2-dev \
        zlib1g-dev \
        liblz4-dev \
        libzstd-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copiar archivos Cargo.toml y Cargo.lock primero para cachear dependencias
COPY ./Cargo.toml ./Cargo.lock ./

# Copiar código
COPY src ./src

# Descargar dependencias
#RUN cargo fetch

# Compilar release
RUN cargo build --release

RUN ls -l target/release

# Entrypoint
CMD ["./target/release/rust_consumer"]