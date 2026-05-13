//! Shared-memory storage backend for the blob cache.
//!
//! This module owns all the raw `pg_sys::dsa_*` / `pg_sys::dshash_*` /
//! LWLock interactions. Everything `unsafe` in the blob cache lives
//! here; the public surface in `super` stays safe Rust.
//!
//! ## Current state
//!
//! Phase 1 (this file): always-miss stubs that compile cleanly and let
//! the cache integration sites be wired into the scan path without
//! changing behaviour. The shmem hook registration is in place but
//! reserves zero bytes and creates no DSA — the actual storage layer
//! is the next layer to land.
//!
//! ## Plan for the storage layer
//!
//! 1. `register_hooks` installs:
//!    - PG 15+: `shmem_request_hook` that calls
//!      `RequestAddinShmemSpace(sizeof(BlobCacheCtl))` and
//!      `RequestNamedLWLockTranche("pg_deltax_blob_cache", n_shards)`.
//!    - PG 14: same calls but inline in `_PG_init`.
//!    - All versions: `shmem_startup_hook` that runs once in the
//!      postmaster's startup, `ShmemInitStruct`s the control block,
//!      `dsa_create_in_place`s the area, and `dshash_create`s the index.
//! 2. `get_pinned` hashes the key, picks the shard, takes the shard's
//!    shared LWLock, looks up the entry, bumps pin_count + last_used,
//!    returns a slice handle.
//! 3. `insert` takes the shard's exclusive LWLock, re-checks the key,
//!    `dsa_allocate`s a size-class-rounded buffer, memcpy's the bytes,
//!    inserts into dshash, links into the LRU head, evicts from LRU
//!    tail if `total_bytes > max_bytes`.
//! 4. `BlobCachePin::Drop` decrements pin_count via the entry's pointer.
//!
//! The interface here is deliberately small so the storage rewrite is
//! self-contained — once it lands, no caller code needs to change.

use super::{BlobCacheKey, BlobCacheStats};

use std::sync::atomic::{AtomicU64, Ordering};

/// Storage-layer handle returned to the public `BlobCachePin`. In the
/// stub it's empty; once storage lands it carries an entry pointer +
/// shard index so `release` can find the right pin counter.
pub(super) struct PinInner;

impl PinInner {
    pub(super) fn as_slice(&self) -> &[u8] {
        // Stub: no entries exist, so this is never called.
        &[]
    }

    pub(super) fn release(&mut self) {
        // Stub: nothing to release.
    }
}

static STATS_MISSES: AtomicU64 = AtomicU64::new(0);

pub(super) fn get_pinned(_key: &BlobCacheKey) -> Option<PinInner> {
    if super::configured_bytes() == 0 {
        return None;
    }
    // Storage backend not yet live — every lookup is a miss.
    STATS_MISSES.fetch_add(1, Ordering::Relaxed);
    None
}

pub(super) fn insert(_key: &BlobCacheKey, _bytes: &[u8]) {
    if super::configured_bytes() == 0 {
        // Cache disabled — nothing to do.
    }
    // Storage backend not yet live — drop on the floor.
}

#[allow(dead_code)] // Surfaced via SRF; lands with the storage backend.
pub(super) fn stats() -> BlobCacheStats {
    BlobCacheStats {
        entries: 0,
        bytes_used: 0,
        bytes_max: super::configured_bytes() as u64,
        hits_total: 0,
        misses_total: STATS_MISSES.load(Ordering::Relaxed),
        evictions_total: 0,
        insert_failures_total: 0,
    }
}

pub(super) fn register_hooks() {
    // Phase 1 placeholder. The real implementation will:
    //   1. (PG 15+) install a `shmem_request_hook` that calls
    //      `RequestAddinShmemSpace` for the BlobCacheCtl struct and
    //      `RequestNamedLWLockTranche` for the per-shard locks.
    //   2. (PG 14)  do the same calls inline here.
    //   3. Install a `shmem_startup_hook` that creates the dsa_area in
    //      place, sized to `configured_bytes()`, and the dshash table
    //      using a custom hash function over BlobCacheKey.
    //
    // No-op at this stage so the extension still loads cleanly and the
    // GUC/integration plumbing can be exercised first.
}
