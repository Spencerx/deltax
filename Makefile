PG_MAJOR ?= 17
DEV_IMAGE  = pg_cocoon-dev:pg$(PG_MAJOR)
IMAGE      = pg_cocoon:pg$(PG_MAJOR)
TARGET_VOL = pg_cocoon_target_pg$(PG_MAJOR)
CARGO_VOL  = pg_cocoon_cargo

PG_VERSIONS ?= 17 18
VENV         = .venv

.PHONY: dev-image image test build clippy run psql cargo clean integration-test \
       bench-clickbench bench-timescaledb-tsl bench-timescaledb-oss bench-timescaledb bench-compare bench-all

# Build the dev toolchain image (rebuilds only when Dockerfile.dev changes)
dev-image:
	docker build -f docker/Dockerfile.dev --build-arg PG_MAJOR=$(PG_MAJOR) -t $(DEV_IMAGE) docker/

# Generic: run any cargo command. Usage: make cargo CMD="check"
cargo: dev-image
	docker run --rm -v $(CURDIR):/build/pg_cocoon -v $(TARGET_VOL):/build/pg_cocoon/target \
		-v $(CARGO_VOL):/usr/local/cargo/registry $(DEV_IMAGE) cargo $(CMD)

test: dev-image
	docker run --rm -v $(CURDIR):/build/pg_cocoon -v $(TARGET_VOL):/build/pg_cocoon/target \
		-v $(CARGO_VOL):/usr/local/cargo/registry $(DEV_IMAGE) sh -c "cargo pgrx test pg$(PG_MAJOR)"

build: dev-image
	docker run --rm -v $(CURDIR):/build/pg_cocoon -v $(TARGET_VOL):/build/pg_cocoon/target \
		-v $(CARGO_VOL):/usr/local/cargo/registry $(DEV_IMAGE) cargo build --features pg$(PG_MAJOR) --no-default-features

clippy: dev-image
	docker run --rm -v $(CURDIR):/build/pg_cocoon -v $(TARGET_VOL):/build/pg_cocoon/target \
		-v $(CARGO_VOL):/usr/local/cargo/registry $(DEV_IMAGE) cargo clippy --features pg$(PG_MAJOR) --no-default-features

# Build the runtime image (production-like, no build tools)
image: dev-image
	docker build -f docker/Dockerfile --build-arg PG_MAJOR=$(PG_MAJOR) -t $(IMAGE) .

# Run postgres with the extension for manual testing
run: image
	docker run --rm --name pg_cocoon -p 5432:5432 -e POSTGRES_PASSWORD=postgres $(IMAGE)

psql:
	docker exec -it pg_cocoon psql -U postgres

$(VENV)/.stamp: tests/requirements.txt
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r tests/requirements.txt
	@touch $@

integration-test: $(VENV)/.stamp
	@for v in $(PG_VERSIONS); do \
		echo "=== Integration tests: PG $$v ==="; \
		$(MAKE) image PG_MAJOR=$$v; \
		PG_COCOON_IMAGE=pg_cocoon:pg$$v $(VENV)/bin/pytest tests/ -v; \
	done

bench-clickbench: $(VENV)/.stamp image
	PG_COCOON_IMAGE=pg_cocoon:pg$(PG_MAJOR) $(VENV)/bin/pytest tests/bench_clickbench.py -v -s

bench-timescaledb-tsl: $(VENV)/.stamp
	TSDB_VARIANT=tsl $(VENV)/bin/pytest tests/bench_clickbench_timescaledb.py -v -s

bench-timescaledb-oss: $(VENV)/.stamp
	TSDB_VARIANT=oss $(VENV)/bin/pytest tests/bench_clickbench_timescaledb.py -v -s

bench-timescaledb: bench-timescaledb-tsl bench-timescaledb-oss

bench-compare: $(VENV)/.stamp
	$(VENV)/bin/python tests/bench_compare.py

bench-all: bench-clickbench bench-timescaledb bench-compare

clean:
	docker volume rm pg_cocoon_target_pg17 pg_cocoon_target_pg18 $(CARGO_VOL) 2>/dev/null || true
	docker builder prune -f --filter type=regular 2>/dev/null || true
