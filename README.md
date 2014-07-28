# Rivers

A library that provides two queues: Delayed (Acheron) and Urgent (Styx). Jobs gets moved form Acheron to Styx by Charon (Ferryman).

### Delayed

Used for scheduled jobs, something that needs to run in the future. Has a built-in ticker that sets a job ready to be popped when schedule is met.

The design is to have two queues, one for each type. Producers can push on both and workers consume only on the urgent queue. For scheduled jobs to reach urgent queue, a Ferryman should be run that's responsible of popping from delayed queue and pushing to urgent queue.

Features:
- Push
- Pop
- Multi-pop
- Size
- Ticker

### Urgent

Used for jobs that needs to run immediately, supports acknowledging and requeuing jobs.

Features:
- Unack Queue
- Push
- Pop
- Multi-push
- Ack
- Requeue

Both queue supports 2 hours age of per second stats of the following:
- Push (Requeue)
- Pop
- Ack
- Size

## Requirements

Requires the following environment variables:

- RIVERS_REDIS_DB
- RIVERS_REDIS_PASSWORD

## Status
- Not used in any real project yet
- Coverage is at 84.6%

## TODO
- Offer way to easily access stats
- Ferryman
