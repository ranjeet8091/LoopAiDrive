const express = require("express");
const { v4: uuidv4 } = require("uuid");

const app = express();
app.use(express.json());

const PORT = 5000;

const priorityValue = { HIGH: 1, MEDIUM: 2, LOW: 3 };

const ingestionStore = {};
let jobQueue = [
  { batch_id: "batch-m-1", ids: [1, 2, 3], status: "yet_to_start" },
  { batch_id: "batch-m-2", ids: [4, 5], status: "yet_to_start" },
];
let processing = false;

app.get("/", (req, res) => {
  res.json(jobQueue)
});
app.post("/ingest", (req, res) => {
  const { ids, priority } = req.body;

  if (!Array.isArray(ids) || !priorityValue[priority]) {
    return res.status(400).json({ error: "Invalid input" });
  }

  const ingestionId = uuidv4();
  const batches = [];

  for (let i = 0; i < ids.length; i += 3) {
    batches.push({
      batch_id: uuidv4(),
      ids: ids.slice(i, i + 3),
      status: "yet_to_start",
    });
  }

  ingestionStore[ingestionId] = {
    ingestion_id: ingestionId,
    status: "yet_to_start",
    priority,
    createdAt: Date.now(),
    batches,
  };

  jobQueue.push({
    ingestionId,
    priority: priorityValue[priority],
    createdAt: Date.now(),
  });

  jobQueue.sort((a, b) => a.priority - b.priority || a.createdAt - b.createdAt);

  if (!processing) processQueue();

  res.json({ ingestion_id: ingestionId });
});

app.get("/status/:ingestion_id", (req, res) => {
  const { ingestion_id } = req.params;
  const data = ingestionStore[ingestion_id];

  if (!data) {
    return res.status(404).json({ error: "Not found" });
  }

  const statuses = data.batches.map((b) => b.status);
  let status = "yet_to_start";
  if (statuses.every((s) => s === "completed")) status = "completed";
  else if (statuses.some((s) => s === "triggered" || s === "completed"))
    status = "triggered";

  res.json({
    ingestion_id: ingestion_id,
    status,
    batches: data.batches,
  });
});

async function processQueue() {
  processing = true;

  while (jobQueue.length > 0) {
    const job = jobQueue.shift();
    const jobData = ingestionStore[job.ingestionId];

    for (const batch of jobData.batches) {
      if (batch.status === "yet_to_start") {
        batch.status = "triggered";
        await new Promise((res) => setTimeout(res, 5000));
        batch.status = "completed";
      }
    }
  }

  processing = false;
}

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
