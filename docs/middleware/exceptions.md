# Exception rules

Without exception rules, a failed handler stops its partition (see [Failures](../runtime/index.md#failures)). Exception rules retry the handler, write the record to a dead letter topic, or skip it.

## Rules

A runtime's `exception_rules` handle a handler's exceptions. potik asks them in order, like `except` clauses, and the first rule that returns a decision handles the exception:

```python
from potik.middleware import on, dead_letter, retry

dead_letters = Topic("orders.dlq", bytes)

runtime = potik.build_runtime(
    config,
    [router],
    exception_rules=[
        on(TimeoutError, retry(max_attempts=5)),
        on(Exception, dead_letter(dead_letters)),
    ],
)
```

Here a `TimeoutError` gets five attempts in total. Once they're used up, the next rule writes the record to `orders.dlq`, and so does any other exception. If no rule decides, the partition stops.

`on(match, rule)` matches an exception type, a tuple of types, or a function that takes the exception and returns `True` or `False`. Pass `topics=` to apply a rule only to some topics' records:

```python
on(KeyError, skip(), topics=[payments])
```

A handler that runs a task group raises an `ExceptionGroup`. When the group holds one exception, e.g. because one task failed and the others were cancelled, the rules see that exception, so `on(TimeoutError, ...)` matches it. A group of several exceptions stays a group. Match it with `on(ExceptionGroup, ...)` or a function.

potik ships these rules:

| Rule | Effect |
| --- | --- |
| `retry(max_attempts=n)` | Calls the handler again, up to `n` calls in total |
| `retry_forever()` | Calls the handler again until it succeeds |
| `dead_letter(topic)` | Commits a copy of the failed record to `topic` instead of the handler's outputs |
| `skip()` | Commits the record's offset with no outputs |
| `reraise()` | Asks no more rules, so the partition stops |

The retry rules wait between calls with exponential backoff, from 0.1s up to 30s. Pass `backoff=ExponentialBackoff(...)` to change it. A retrying record holds up only its own partition.

The rules run inside the runtime's `middleware`. So tracing and metrics see one handler call per record, with its retries, and the outcome after the rules.

## Dead letters

A dead letter keeps the failed record's key, value, and headers as raw bytes, even when they don't decode. potik adds `potik.dlq.*` headers with the source topic, partition, and offset, the exception's class and message, and the number of attempts. The exception message may contain data from the record, and potik cuts it to 1000 characters.

To set other headers, pass a function that returns them:

```python
from potik.middleware import ExceptionContext, dead_letter_headers


def without_message(context: ExceptionContext[Exception]) -> list[tuple[str, str | None]]:
    return [(name, value) for name, value in dead_letter_headers(context) if name != "potik.dlq.exception.message"]


on(Exception, dead_letter(dead_letters, headers=without_message))
```

## Errors that retrying can't fix

`retry()` and `retry_forever()` leave any `PermanentError` to the next rule, because the same record would fail the same way again. `DecodeError` and `EncodeError` are permanent. So is `InvalidOutputError`, which a handler raises when it returns anything but output records. Subclass `PermanentError` for your own errors of this kind.

To stop the partition on an error that a later rule would handle, put `reraise()` before that rule. It works like a bare `raise` before `except Exception:`:

```python
exception_rules = [
    on(InvalidOutputError, reraise()),  # a bug: stop this partition
    on(Exception, dead_letter(dead_letters)),
]
```

## Writing a rule

A rule is a function that takes an `ExceptionContext` with the `exception`, the input `record`, and the `attempt` number. It may be `async`. It returns one of these:

- `RetryAfter(seconds)` to call the handler again after `seconds`.
- A list of records to commit in place of the handler's outputs.
- `Reraise()` to ask no more rules and let the exception propagate.
- `None` to leave the exception to the next rule.

This rule waits as long as a rate-limited API asks:

```python
def respect_retry_after(context: ExceptionContext[RateLimited]) -> RetryAfter:
    return RetryAfter(context.exception.retry_after)


exception_rules = [
    on(RateLimited, respect_retry_after),
    on(Exception, dead_letter(dead_letters)),
]
```

A rule that returns records to a topic other than a dead letter topic should declare it, so the [topic check](topic-check.md) covers it. Pass the topic to `Router(output_topics=...)`.

## Core runtime

In a [core runtime](../runtime/core.md), pass the rules to an `ExceptionMiddleware`. Its `output_topics` lists the dead letter topics, for the topic check:

```python
exceptions = ExceptionMiddleware(
    on(TimeoutError, retry(max_attempts=5)),
    on(Exception, dead_letter("orders.dlq")),
)
```
