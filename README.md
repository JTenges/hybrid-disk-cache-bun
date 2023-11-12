# hybrid-disk-cache-bun

A hybrid disk cache library that utilized both the solid SQLite3 database and the file system.

```bash
bun add hybrid-disk-cache-bun
```

When the value is larger than 10 kilobytes, it will be written to the file system, otherwise saved in SQLite3 database.

The benefits of using this kind of hybrid cache are:

- Always use the small footprint and high performace SQLite3 index.
- Using file system for larger files. No need to run `vacuum` for releasing space

Also, here are some bonus:

- 100% test coverage
- Pure Typescript
- Used in production with 300K keys
- SQLite3's indices will always be used when searching for a key. (which is FAST)

A fork of [hybrid-disk-cache](https://github.com/next-boost/hybrid-disk-cache) using bun's sqlite api.

## APIs

```javascript
// tbd, time before deletion: This is used to control how long a key
// should remain in the cache after expired (ttl)
// And `cache.purge` will delete all records with ttl + tbd < now
const cache = new Cache({ path, ttl, tbd })

// set. if ttl empty, use the cache's ttl
cache.set(key, value)
// set. will expire in 5 seconds
cache.set(key, value, 5)

// get
cache.get(key, defaultValue)

// del
cache.del(key)

// check cache availability and status
// status in 'miss' | 'stale' | 'hit'
const status = cache.has(key)

// if you want to serve even the stale value
if (cache.has(key) !== 'miss') {
    const value = cache.get(key)
}

// if you only want the unexpired one
if (cache.has(key) === 'hit') {
    const value = cache.get(key)
}

// delete all expired keys
cache.purge()
```

Check [`index.test.ts`](https://github.com/JTenges/hybrid-disk-cache-bun/blob/master/test/index.test.ts) for examples.

## Benchmarks

TODO

## License

MIT. Copyright 2023, Joji Tenges.
