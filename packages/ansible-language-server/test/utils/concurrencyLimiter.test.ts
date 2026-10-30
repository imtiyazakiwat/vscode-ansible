import { afterEach, describe, expect, it } from "vitest";
import { ansibleLintConcurrencyLimiter as limiter } from "@src/utils/concurrencyLimiter.js";

interface Gate {
  promise: Promise<void>;
  open: () => void;
}

function gate(): Gate {
  let open!: () => void;
  const promise = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { promise, open };
}

/** Lets queued microtasks (acquire/release chains) settle. */
async function flush(): Promise<void> {
  for (let i = 0; i < 5; i++) {
    await Promise.resolve();
  }
}

/**
 * Schedules `count` tasks that each block on their own gate, tracking how
 * many run at once and the order in which they start.
 */
function schedule(count: number) {
  const gates = Array.from({ length: count }, gate);
  const started: number[] = [];
  let running = 0;
  let peak = 0;
  const results = gates.map((g, index) =>
    limiter.run(async () => {
      running++;
      peak = Math.max(peak, running);
      started.push(index);
      await g.promise;
      running--;
      return index;
    }),
  );
  return {
    gates,
    started,
    results,
    get running() {
      return running;
    },
    get peak() {
      return peak;
    },
  };
}

describe("ansibleLintConcurrencyLimiter", function () {
  afterEach(function () {
    limiter.setLimit(4);
  });

  it("returns the task result", async function () {
    limiter.setLimit(2);
    await expect(limiter.run(() => Promise.resolve("ok"))).resolves.toBe("ok");
  });

  it("never runs more tasks than the limit and starts queued tasks in FIFO order", async function () {
    limiter.setLimit(2);
    const tasks = schedule(5);
    await flush();
    expect(tasks.started).toEqual([0, 1]);

    tasks.gates[0].open();
    await flush();
    expect(tasks.started).toEqual([0, 1, 2]);
    expect(tasks.running).toBe(2);

    for (const g of tasks.gates) {
      g.open();
    }
    await expect(Promise.all(tasks.results)).resolves.toEqual([0, 1, 2, 3, 4]);
    expect(tasks.started).toEqual([0, 1, 2, 3, 4]);
    expect(tasks.peak).toBe(2);
  });

  it("releases the slot when a task rejects", async function () {
    limiter.setLimit(1);
    const failing = limiter.run(() => Promise.reject(new Error("lint failed")));
    const next = limiter.run(() => Promise.resolve("next"));

    await expect(failing).rejects.toThrow("lint failed");
    await expect(next).resolves.toBe("next");
  });

  it("releases the slot when a task throws synchronously", async function () {
    limiter.setLimit(1);
    const failing = limiter.run(() => {
      throw new Error("spawn failed");
    });
    const next = limiter.run(() => Promise.resolve("next"));

    await expect(failing).rejects.toThrow("spawn failed");
    await expect(next).resolves.toBe("next");
  });

  it("starts queued tasks immediately when the limit is raised", async function () {
    limiter.setLimit(1);
    const tasks = schedule(3);
    await flush();
    expect(tasks.started).toEqual([0]);

    limiter.setLimit(3);
    await flush();
    expect(tasks.started).toEqual([0, 1, 2]);

    for (const g of tasks.gates) {
      g.open();
    }
    await Promise.all(tasks.results);
  });

  it("lets running tasks finish but holds new ones when the limit is lowered", async function () {
    limiter.setLimit(3);
    const tasks = schedule(4);
    await flush();
    expect(tasks.started).toEqual([0, 1, 2]);

    limiter.setLimit(1);
    tasks.gates[0].open();
    tasks.gates[1].open();
    await flush();
    expect(tasks.started).toEqual([0, 1, 2]);

    tasks.gates[2].open();
    await flush();
    expect(tasks.started).toEqual([0, 1, 2, 3]);

    tasks.gates[3].open();
    await Promise.all(tasks.results);
  });

  it.each([
    ["zero", 0],
    ["a negative number", -3],
    ["NaN", Number.NaN],
    ["Infinity", Number.POSITIVE_INFINITY],
  ])("treats %s as a limit of one", async function (_label, value) {
    limiter.setLimit(value);
    const tasks = schedule(2);
    await flush();
    expect(tasks.started).toEqual([0]);

    tasks.gates[0].open();
    tasks.gates[1].open();
    await Promise.all(tasks.results);
    expect(tasks.peak).toBe(1);
  });

  it("rounds fractional limits down", async function () {
    limiter.setLimit(2.9);
    const tasks = schedule(3);
    await flush();
    expect(tasks.started).toEqual([0, 1]);

    for (const g of tasks.gates) {
      g.open();
    }
    await Promise.all(tasks.results);
  });
});
