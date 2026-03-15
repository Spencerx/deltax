ARG PG_MAJOR=17

# ==============================================================================
# Build — reuses the dev image (already has Rust, cargo-pgrx, PG dev headers)
# ==============================================================================
FROM pg_deltax-dev:pg${PG_MAJOR} AS builder

COPY --chown=builder:builder . /build/pg_deltax

USER builder
RUN cargo pgrx package --pg-config /usr/lib/postgresql/${PG_MAJOR}/bin/pg_config \
        --features pg${PG_MAJOR} --no-default-features

# ==============================================================================
# Runtime
# ==============================================================================
FROM postgres:${PG_MAJOR}-bookworm

ARG PG_MAJOR=17

COPY --from=builder /build/pg_deltax/target/release/pg_deltax-pg${PG_MAJOR}/ /
