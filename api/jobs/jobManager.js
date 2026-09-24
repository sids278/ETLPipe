import { randomUUID } from 'node:crypto';
import { httpError } from '../httpError.js';

// In-memory job store. Jobs are lost when the API restarts; one job runs at a time so
// seeding and syncing never overlap and skew each other's timings.
const jobs = new Map();
let activeJobId = null;

// Starts `run(ctx)` in the background and returns the job right away.
// `run` writes progress into ctx.progress and should stop early when ctx.isCancelled() is true.
export function startJob(type, params, run) {
  if (activeJobId) throw httpError(409, `Job ${activeJobId} is still running; cancel it or wait for it to finish`);

  const job = {
    id: randomUUID(),
    type,
    params,
    status: 'running',
    startedAt: Date.now(),
    finishedAt: null,
    progress: {},
    result: null,
    error: null,
    cancelRequested: false,
  };
  jobs.set(job.id, job);
  activeJobId = job.id;

  const ctx = { progress: job.progress, isCancelled: () => job.cancelRequested };
  Promise.resolve()
    .then(() => run(ctx))
    .then((result) => {
      job.result = result;
      job.status = job.cancelRequested ? 'cancelled' : 'succeeded';
    })
    .catch((err) => {
      console.error(`job ${job.id} (${type}) failed:`, err);
      job.error = err.message;
      job.status = 'failed';
    })
    .finally(() => {
      job.finishedAt = Date.now();
      activeJobId = null;
    });

  return view(job);
}

export function getJob(id) {
  const job = jobs.get(id);
  if (!job) throw httpError(404, `No job with id ${id}`);
  return view(job);
}

export function listJobs() {
  return [...jobs.values()].reverse().map(view);
}

export function cancelJob(id) {
  const job = jobs.get(id);
  if (!job) throw httpError(404, `No job with id ${id}`);
  if (job.status !== 'running') throw httpError(409, `Job ${id} is already ${job.status}`);
  job.cancelRequested = true;
  return view(job);
}

function view(job) {
  return {
    id: job.id,
    type: job.type,
    status: job.cancelRequested && job.status === 'running' ? 'cancelling' : job.status,
    params: job.params,
    startedAt: new Date(job.startedAt).toISOString(),
    finishedAt: job.finishedAt ? new Date(job.finishedAt).toISOString() : null,
    elapsedSeconds: Math.round(((job.finishedAt ?? Date.now()) - job.startedAt) / 100) / 10,
    progress: job.progress,
    result: job.result,
    error: job.error,
  };
}
