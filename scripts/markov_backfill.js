const { BigQuery } = require("@google-cloud/bigquery");

const args = Object.fromEntries(
  process.argv.slice(2).map((arg) => {
    const [key, ...rest] = arg.replace(/^--/, "").split("=");
    return [key, rest.join("=") || "true"];
  })
);

const projectId = args.project || "bigquery-2024";
const keyFilename = args.keyfile;
const startDate = args.start;
const endDate = args.end;

if (!startDate || !endDate) {
  console.error("Usage: node scripts/markov_backfill.js --start=2026-01-01 --end=2026-06-01 [--keyfile=path]");
  process.exit(1);
}

const bigquery = new BigQuery({
  projectId,
  ...(keyFilename ? { keyFilename } : {}),
});

const sourceTable = `\`${projectId}.attribution.session_source_medium\``;
const targetTable = `\`${projectId}.attribution_v1.mta_markov_weights_daily\``;

function addDays(date, days) {
  const d = new Date(`${date}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function dateRange(start, end) {
  const dates = [];
  for (let d = start; d <= end; d = addDays(d, 1)) dates.push(d);
  return dates;
}

function sqlString(value) {
  return String(value).replace(/'/g, "''");
}

function compactPath(channels) {
  const out = [];
  for (const channel of channels || []) {
    if (!channel) continue;
    if (out[out.length - 1] !== channel) out.push(channel);
  }
  return out;
}

function buildTransitionModel(paths) {
  const transitionCounts = new Map();
  const states = new Set(["(start)"]);
  const channelCounts = new Map();

  for (const rawPath of paths) {
    const channels = compactPath(rawPath);
    if (!channels.length) continue;
    for (const channel of channels) {
      states.add(channel);
      channelCounts.set(channel, (channelCounts.get(channel) || 0) + 1);
    }
    const fullPath = ["(start)", ...channels, "(conversion)"];
    for (let i = 0; i < fullPath.length - 1; i += 1) {
      const from = fullPath[i];
      const to = fullPath[i + 1];
      if (!transitionCounts.has(from)) transitionCounts.set(from, new Map());
      const row = transitionCounts.get(from);
      row.set(to, (row.get(to) || 0) + 1);
    }
  }

  const transitions = new Map();
  for (const [from, row] of transitionCounts.entries()) {
    const total = [...row.values()].reduce((sum, value) => sum + value, 0);
    transitions.set(
      from,
      new Map([...row.entries()].map(([to, count]) => [to, count / total]))
    );
  }

  return {
    states: [...states],
    channels: [...channelCounts.keys()],
    transitions,
  };
}

function conversionProbability(model, removedChannel) {
  const values = new Map(model.states.map((state) => [state, 0]));
  values.set("(conversion)", 1);

  for (let iter = 0; iter < 500; iter += 1) {
    let maxDelta = 0;
    for (const state of model.states) {
      if (state === removedChannel) continue;
      const row = model.transitions.get(state);
      if (!row) continue;
      let nextValue = 0;
      for (const [to, probability] of row.entries()) {
        if (to === removedChannel) continue;
        nextValue += probability * (to === "(conversion)" ? 1 : (values.get(to) || 0));
      }
      maxDelta = Math.max(maxDelta, Math.abs(nextValue - (values.get(state) || 0)));
      values.set(state, nextValue);
    }
    if (maxDelta < 1e-12) break;
  }

  return values.get("(start)") || 0;
}

function calculateWeights(paths) {
  const model = buildTransitionModel(paths);
  const baseProbability = conversionProbability(model);
  const effects = model.channels.map((channel) => {
    const removedProbability = conversionProbability(model, channel);
    const removalEffect = baseProbability > 0
      ? Math.max(0, (baseProbability - removedProbability) / baseProbability)
      : 0;
    return { channel, removalEffect };
  });

  const totalEffect = effects.reduce((sum, row) => sum + row.removalEffect, 0);
  if (totalEffect <= 0) {
    const fallbackWeight = effects.length ? 1 / effects.length : 0;
    return effects.map((row) => ({ ...row, weight: fallbackWeight }));
  }

  return effects.map((row) => ({
    ...row,
    weight: row.removalEffect / totalEffect,
  }));
}

async function ensureTargetTable() {
  await bigquery.query({
    location: "US",
    query: `
      CREATE TABLE IF NOT EXISTS ${targetTable} (
        weight_date DATE NOT NULL,
        window_start_date DATE NOT NULL,
        window_end_date DATE NOT NULL,
        channel STRING NOT NULL,
        removal_effect FLOAT64,
        weight FLOAT64,
        run_timestamp TIMESTAMP NOT NULL
      )
      PARTITION BY weight_date
      CLUSTER BY channel
    `,
  });
}

async function loadPaths(weightDate) {
  const windowStart = addDays(weightDate, -29);
  const [rows] = await bigquery.query({
    location: "US",
    query: `
      SELECT
        ARRAY_AGG(channel ORDER BY event_date) AS channels
      FROM ${sourceTable}
      WHERE purchase_date BETWEEN @windowStart AND @weightDate
        AND channel IS NOT NULL
        AND transaction_id IS NOT NULL
      GROUP BY unified_user_id, transaction_id
    `,
    params: { windowStart, weightDate },
  });
  return { windowStart, paths: rows.map((row) => row.channels || []) };
}

async function mergeWeights(weightDate, windowStart, weights) {
  if (!weights.length) {
    console.warn(`${weightDate}: no paths, skipped`);
    return;
  }

  const runTimestamp = new Date().toISOString();
  const rowsSql = weights.map((row) => `
    SELECT
      DATE '${weightDate}' AS weight_date,
      DATE '${windowStart}' AS window_start_date,
      DATE '${weightDate}' AS window_end_date,
      '${sqlString(row.channel)}' AS channel,
      ${row.removalEffect} AS removal_effect,
      ${row.weight} AS weight,
      TIMESTAMP('${runTimestamp}') AS run_timestamp
  `).join("\nUNION ALL\n");

  await bigquery.query({
    location: "US",
    query: `
      MERGE ${targetTable} AS target
      USING (${rowsSql}) AS source
      ON target.weight_date = source.weight_date
        AND target.channel = source.channel
      WHEN MATCHED THEN UPDATE SET
        window_start_date = source.window_start_date,
        window_end_date = source.window_end_date,
        removal_effect = source.removal_effect,
        weight = source.weight,
        run_timestamp = source.run_timestamp
      WHEN NOT MATCHED THEN INSERT (
        weight_date,
        window_start_date,
        window_end_date,
        channel,
        removal_effect,
        weight,
        run_timestamp
      ) VALUES (
        source.weight_date,
        source.window_start_date,
        source.window_end_date,
        source.channel,
        source.removal_effect,
        source.weight,
        source.run_timestamp
      )
    `,
  });
}

async function processDate(weightDate) {
  const { windowStart, paths } = await loadPaths(weightDate);
  const weights = calculateWeights(paths);
  const weightSum = weights.reduce((sum, row) => sum + row.weight, 0);
  await mergeWeights(weightDate, windowStart, weights);
  console.log(`${weightDate}: paths=${paths.length}, channels=${weights.length}, weight_sum=${weightSum.toFixed(8)}`);
}

(async () => {
  await ensureTargetTable();
  for (const date of dateRange(startDate, endDate)) {
    await processDate(date);
  }
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
