import { randomUUID } from "node:crypto";
import { Client } from "faktory-worker";

const [mode, jid] = process.argv.slice(2);
const client = new Client({ url: process.env.FAKTORY_URL, wid: randomUUID().slice(0, 8) });

await client.connect();

const runtime = process.versions.bun ? "bun" : "node";

if (mode === "beat") {
  let seen;

  for (;;) {
    const state = await client.beat();

    if (state !== "OK" && state !== seen) {
      console.log(`${runtime} beat ${state}`);
    }

    seen = state;

    if (state === "terminate") {
      break;
    }

    await new Promise((resolve) => setTimeout(resolve, 100));
  }
} else if (mode === "push") {
  await client.push({ jid, jobtype: "compat", args: ["node", 42], queue: "default" });
} else {
  const job = await client.fetch("default");

  if (!job || job.jid !== jid) {
    throw new Error(`unexpected job: ${JSON.stringify(job)}`);
  }

  if (mode === "fail") {
    await client.fail(jid, new Error("compat failure"));
    process.exit(0);
  }

  await client.ack(jid);
  console.log(`${runtime} consumed ${jid} with args ${JSON.stringify(job.args)}`);
}

await client.close();
process.exit(0);
