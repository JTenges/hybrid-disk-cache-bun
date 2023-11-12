import { describe, it, expect } from "bun:test";
import fs from "fs";
import Cache from "../src";

export const sleep = async (t: number): Promise<void> => {
  return new Promise((resolve) => setTimeout(resolve, t));
};

describe("disk cache with ttl", () => {
  it("init", () => {
    const cache = new Cache();
    expect(fs.existsSync(cache.path)).toBeTrue();
  });

  it("init with params", () => {
    let cache = new Cache({ path: "/tmp", ttl: 100, tbd: 300 });
    expect(cache.ttl).toEqual(100);
    expect(cache.tbd).toEqual(300);
    expect(cache.path).toEqual("/tmp");

    delete process.env.TMPDIR;
    cache = new Cache({ ttl: 100, tbd: 300 });
    expect(cache.path).toBe("/tmp/hdc");
  });

  it("set / get", async () => {
    const cache = new Cache();
    const v = Buffer.from("B");
    await cache.set("A", v);
    expect(await cache.get("A")).toEqual(v);

    const v2 = Buffer.from("AAA");
    await cache.set("A", v2);
    expect(await cache.get("A")).toEqual(v2);

    const defaultValue = Buffer.from("AA");
    expect(await cache.get("B", defaultValue)).toEqual(defaultValue);
  });

  it("set / get stale / hit / miss", async () => {
    const cache = new Cache();
    const key = "key:1";
    await cache.set(key, Buffer.from("1"), 0.8);
    let s = await cache.has(key);
    expect(s).toEqual("hit");
    await sleep(1000);
    s = await cache.has(key);
    expect(s).toEqual("stale");
    const v = await cache.get(key);
    expect(v).toEqual(Buffer.from("1"));
    s = await cache.has("key:2");
    expect(s).toEqual("miss");
  });

  it("set / get large buffer", async () => {
    const cache = new Cache();
    const key1 = "key:l1";
    const d = new Array(20000).fill("A");
    const buf = Buffer.from(d);
    await cache.set(key1, buf, 0.8);
    expect(await cache.get(key1)).toEqual(buf);
  });

  it("del / get miss", async () => {
    const cache = new Cache();
    cache.set("A", Buffer.from("1"));
    expect(await cache.get("A")).toEqual(Buffer.from("1"));
    await cache.del("A");
    expect(await cache.get("A")).toBeUndefined();
    await cache.del("not-exist");
  });

  it("purge", async () => {
    const cache = new Cache({ ttl: 0.1, tbd: 0.1 });
    const key1 = "key:l1";
    const d = new Array(20000).fill("A");
    const buf = Buffer.from(d);
    await cache.set(key1, buf);
    expect(await cache.get(key1)).toEqual(buf);
    await sleep(500);
    await cache.purge();
    expect(await cache.get(key1)).toBeUndefined;
  });
});
