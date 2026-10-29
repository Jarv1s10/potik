# potik

An async framework for Kafka services that consume, process, and produce records.

## Language

### Core

The runtime that consumes, handles, and commits records, without typed handlers: a Runtime runs once, calling one function of raw records and committing what it returns. It also holds the Producer and the health probes. The built-in middleware sits on top of it, and the Typed layer on top of both. Like Starlette under FastAPI, it is usable on its own.

### Typed layer

Routers, Topics, and typed handlers, whose parameters take decoded parts of a record, and the function that builds a Core Runtime from them. It adds what the Core leaves to its users: dispatch by topic, per-handler limits, the Run context, and declared Output topics. It has no App object: a service builds its resources, then its runtime.

### Run

One execution of a Runtime, from its start until it shuts down or fails. A Runtime has exactly one run, so another run, e.g. after a failure, needs a new Runtime and new Middleware instances.

### Handler

The function that a run calls for each input record. A core handler takes the raw record and returns a list or tuple of Producer records. A typed handler takes decoded parts of the record through its parameters, and returns None, one Producer record, or several.

### Topic

In the Typed layer, the declaration of a Kafka topic: its name, and the types and Serdes of its keys and values. Producers and consumers of the topic share one declaration, so they agree on its wire format.

### Serde

What converts a Topic's keys or values between bytes and Python types, once per record and without I/O. An async serde does I/O to decode a record, e.g. fetching its writer schema from a registry, then decodes like any other serde.

### Parameter marker

An annotation, `Key()`, `Header(name)`, or `Context()`, that tells a typed handler's parameter which part of the record it takes. The one parameter without a marker takes the value, and a parameter typed as the record takes the record.

### Producer record

A record to produce, e.g. made by calling a Topic. It becomes either an Output or a Send.

### Output

A producer record that a handler returns. It commits in the same transaction as its input's offset, so it becomes visible exactly once, and only after that commit.

### Send

A producer record sent right away through a Producer, independently of any transaction. Sends from a handler are at-least-once: a replayed or retried handler sends again, and only their consumers can deduplicate them.

### Handled record

An input record whose handler has returned, and whose Outputs and next offset wait for their Transaction. It is not committed yet, so it replays if the run stops before its Transaction commits. In a topic that runs by Key concurrency, it first waits for every earlier record of its partition to finish, so a Transaction never commits past an unfinished record.

### Key concurrency

A topic's opt-in to running its records by key: records with the same key run one at a time, in offset order, and records with other keys run concurrently, up to the topic's number of keys in progress per instance. All null keys count as one key. Without it, each partition runs one record at a time.

### Transaction

The atomic commit of a batch of Handled records' Outputs and offsets, from all partitions. It carries the consumer group's metadata, so the group rejects it from a stale member, e.g. one that has lost its partitions.

### Replay

Handling an input record again because its offset never committed, e.g. after a failed run, a revocation that cancelled its handler, or a Stopped partition's reassignment. Its earlier Outputs never became visible, but its earlier Sends may have.

### Output topic

A topic that a handler, router, or middleware declares it returns Outputs to. The Typed layer adds it to the Topic check, but Outputs to undeclared topics are still allowed. The core has no declarations.

### Topic check

The startup check that a run's handled topics and output topics, or a Producer's topics, exist. `TopicCheckMiddleware` does it in its lifespan, so a missing topic fails the start before any record is handled or sent. The Typed layer adds it by default. A core Runtime or a Producer has it only if its middleware includes it, with the topics to check.

### Producer

A non-transactional client that sends records to Topics, with or without a Runtime. Whoever needs it owns it and opens it once, e.g. an API service's lifespan, or a service's `main`, which shares it with handlers through the Run context.

### Reply listener

A client that awaits replies to requests in the process that sent them, e.g. each replica of an API. It reads every partition of a reply topic without a consumer group, so every replica sees every reply and keeps those it awaits. It does not send requests, so the app sends them, e.g. through a Producer.

### Responder

The service that handles a request and returns its reply as an Output, copying the request's Correlation ID. It is a plain handler, so its reply commits with the request's offset.

### Correlation ID

The ID that pairs a reply with its request. The app chooses where it travels, e.g. a header or a field of the value. potik sets no convention, and the Reply listener reads it from each reply with the app's `correlate` function.

### Stage

One of the points where middleware wraps a run or a Producer: the Lifespan, each handler call, each Transaction, and each Send. Only the handler call stage may recover from a failure, e.g. by retrying or replacing the Outputs.

### Lifespan

A middleware's work around a run, or around the time a Producer is open: before its clients start and after they close. Its exit always runs to the end, even after a cancellation, and it cannot suppress the run's failure. A service's own resources, e.g. a connection pool, live around the Runtime instead, in its own code.

### Lifespan scope

What a middleware's lifespan surrounds: a `RunScope`, with the run's config and live status, or a `ProducerScope`, with the Producer's connection options. It lets middleware, e.g. the Topic check, metrics, or health probes, learn about a run without arguments of their own.

### Run context

What a service passes to the Typed layer's runtime for its handlers, e.g. its services and their clients. A handler takes it through a `Context()` parameter. Middleware takes the resources it needs as its own arguments instead.

### Exception rule

A function that decides what follows a handler's exception: call the handler again, commit other Outputs in place of the failed call's, or let the exception propagate. Rules are asked in order, like `except` clauses, and an exception that no rule decides on stops the partition. A rule sees the single exception of an exception group that holds only one.

### Permanent error

A handler failure that would repeat if the handler ran again, e.g. a value that does not decode. Retry rules pass it to the next Exception rule instead of retrying it.

### Dead letter

A copy of a failed input record, with headers that describe the failure, committed to a dead letter topic as the record's Outputs. It lets the partition move past the record without losing it.

### Stopped partition

A partition stopped at a record whose handler failed, or that it could not fetch, with nothing past that record committed. It resumes, handling that record again, only after a reassignment or a restart. Do not call it "isolated", which suggests Kafka's isolation level, or "paused", which is a Paused partition.

### Paused partition

A partition whose fetching the runtime paused because its buffer of fetched records is full. It resumes on its own as its handler drains the buffer, unlike a Stopped partition, which is also paused but never resumes in the same assignment.

### Offset policy

The choice of where a newly assigned partition starts, usually its committed offset. A start that differs from the committed offset commits before any record is handled, so a restart resumes from it.

### Revocation

The group taking partitions back in an orderly rebalance. The runtime lets their in-flight handlers finish within a grace period, cancels the rest, and commits their Handled records before it gives the partitions up.

### Lost partitions

Partitions that the group took away without a Revocation, so another member may already own them. Losing partitions fails the run, which commits nothing more.

### Shutdown

The graceful end of a run, e.g. on SIGTERM or when its task is cancelled. The runtime stops polling, lets in-flight handlers finish within a grace period, cancels the rest, commits the Handled records, and leaves the group.

### Liveness and readiness

The two health probes of a run. Liveness fails only when a restart would help, i.e. the run failed or stopped polling or committing, never for a slow handler, a broker outage alone, or a Stopped partition. Readiness passes while the run runs, with or without partitions.

### Middleware instance

One middleware object, which serves one Runtime or one Producer, once, so its state lasts one run. Another run, or another owner, needs a new instance.
