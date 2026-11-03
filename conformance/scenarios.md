# Client Conformance Scenarios

Each client must pass these scenarios against a real server. A client maps each ID to one test.

| ID | Scenario |
|---|---|
| C01 | Connect with a `tcp://` URL, and read `ANWAY_URL` when the URL is blank. |
| C02 | Connect with a password. A wrong password returns an error. |
| C03 | Connect with `tcp+tls://`. |
| C04 | Push, fetch, and ack one job. Args keep their values. |
| C05 | Fetch returns no job and no error when the queue is empty. |
| C06 | Fail moves a job to retries and keeps the error type and message. |
| C07 | A second push of a unique job returns the not-unique error kind. |
| C08 | A push with `at` does not reach the queue before that time. |
| C09 | Pause stops fetch, and resume allows it. |
| C10 | Mutate requeues retries. |
| C11 | A batch runs complete, then success. Status shows the totals. |
| C12 | Track set and get return percent and desc. |
| C13 | Info returns queue sizes. |
| C14 | One client works from many concurrent callers. |
| C15 | A client reconnects after the server restarts. |
| C16 | A call on a closed client returns the closed error kind. |
| W01 | A worker runs a registered job and acks it. |
| W02 | A handler error fails the job. |
| W03 | A handler panic or exception fails the job, and the worker continues. |
| W04 | An unknown job type fails the job. |
| W05 | Middleware wraps each job in order. |
| W06 | The worker stops at `terminate` in a `BEAT` reply and fails unfinished jobs with a `FAIL`. A scripted server sends `terminate`, because a real server sends it only for an operator. |
| W07 | The worker stops at a local stop request and lets running jobs finish. |
| W08 | Server `quiet` stops fetch, and the worker stays alive until `terminate`. |
| W09 | The shutdown timeout cancels the context of running jobs, and those jobs fail. |
| W10 | A handler that ignores cancellation cannot block the shutdown. Its job fails, and a late return does not change that result. |
| W11 | A server restart does not stop the worker. The worker reconnects and runs a job pushed after the restart. |
| W12 | The worker sends its thread count in `BEAT`. When the reply has `concurrency`, it starts or stops threads, lets a thread that stops finish its job, and reports the new count in a `BEAT` at once, not at the next interval. |
