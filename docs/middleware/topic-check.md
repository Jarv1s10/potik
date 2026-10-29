# Topic check

`TopicCheckMiddleware` checks that topics exist before a run starts consuming. A missing topic raises `TopicCheckError`, so a typo fails the deployment instead of leaving the app idle. The check never creates topics.

`potik.build_runtime()` adds the check by default, and checks these topics:

- the topics that its handlers read,
- the output topics that handlers and routers declare with `output_topics=`,
- the dead letter topics of the exception rules,
- the topics that other middleware declares in `output_topics`.

Set `potik.build_runtime(..., check_topics=False)` to skip the check, for example when consumers start before their topics exist.

## Producers

A `Producer` checks nothing by default. Pass it a `TopicCheckMiddleware` with the topics it sends to, and opening the producer fails if one is missing:

```python
producer = Producer("broker:9092", middleware=[TopicCheckMiddleware([orders])])
```

## Core runtime

In a [core runtime](../runtime/core.md), `TopicCheckMiddleware` checks only the topics that you pass, so include the output and dead letter topics. Put it first in the middleware list:

```python
TopicCheckMiddleware([*handlers, "orders.accepted", *exceptions.output_topics])
```
