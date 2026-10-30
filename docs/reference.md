# API reference

`potik` also re-exports the core names that services use, such as `Config`, `Producer`, `Runtime`, and the record types. Each name is documented once, under the layer that defines it.

## Runtime

::: potik.core

::: potik.core.health

## Middleware

::: potik.middleware

::: potik.middleware.tracing
    options:
      members: [TracingMiddleware]

::: potik.middleware.metrics
    options:
      members: [MetricsMiddleware]

## Typed API

::: potik
    options:
      members: [build_runtime, Router, RegistrationError, Topic, OutputTopic, HandlerResult, Key, Header, Context, Depends, DecodeError, EncodeError]

::: potik.serde
    options:
      members: [Serde, AsyncSerde, SerdeContext, Decoder, Encoder, Resolver, Text, PydanticJson, MsgspecJson]

::: potik.testing
    options:
      members: [MockCluster]
