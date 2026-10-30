# Batches and Tracking

## Batches

A batch groups jobs and runs callback jobs when they finish.

1. `BATCH NEW` creates a batch with a `success` callback, a `complete` callback, or both.
2. Each job has `custom.bid`.
3. `BATCH COMMIT` closes the definition.

After a commit, a `PUSH` to the batch fails. `BATCH OPEN` allows new jobs and child batches until the next commit. It fails after the `success` callback fires.

### Callbacks

| Callback | Fires when |
|---|---|
| `complete` | All jobs ran one time, with `ACK` or `FAIL`, and all child batches are complete. |
| `success` | All jobs have `ACK`, and the `success` callback of all child batches has `ACK`. |

- `success` always follows `complete`.
- An empty batch fires its callbacks at `BATCH COMMIT`.
- Callback jobs have `custom._bid` and `custom._cb`. They run and retry like other jobs.
- A callback counts as finished only at its `ACK`. A missing callback counts as finished at once.

### Status

`pending` counts jobs without `ACK`. `failed` counts pending jobs that failed at least one time. `complete_st` and `success_st` are blank before a callback fires, `1` after it fires, and `2` after it finishes. These state values are a project choice.

### Limits

- A job that goes to the dead set, or that a `MUTATE` or an expiry removes, stays pending. The `success` callback then does not fire.
- A batch without a commit expires after 30 minutes. A reopened batch has no expiry.
- A batch expires 24 hours after its `success` callback finishes. Unfinished batches do not expire.

## Tracking

A job with `custom.track = 1` has a track.

- **States:** `unknown`, `enqueued`, `working`, `success`, `failed`, and `dead`.
- **`TRACK SET`:** Updates `percent` and `desc`. `reserve_until` extends a reservation and cannot make it sooner.
- **Expiry:** A track expires 30 minutes after its last state change, or 30 minutes after the reservation ends. `TRACK GET` then returns `unknown`.
