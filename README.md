# Kafka Eventing Components
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/knative.dev/eventing-kafka)
[![Go Report Card](https://goreportcard.com/badge/knative-sandbox/eventing-kafka)](https://goreportcard.com/report/knative-sandbox/eventing-kafka)
[![Releases](https://img.shields.io/github/release-pre/knative-sandbox/eventing-kafka.svg)](https://github.com/knative-sandbox/eventing-kafka/releases)
[![LICENSE](https://img.shields.io/github/license/knative-sandbox/eventing-kafka.svg)](https://github.com/knative-sandbox/eventing-kafka/blob/master/LICENSE)
[![codecov](https://codecov.io/gh/knative-sandbox/eventing-kafka/branch/master/graph/badge.svg)](https://codecov.io/gh/knative-sandbox/eventing-kafka)
[![TestGrid](https://img.shields.io/badge/testgrid-eventing_kafka-informational?logo=data:image/x-icon;base64,AAABAAEAICAAAAEACACoCAAAFgAAACgAAAAgAAAAQAAAAAEACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAALAAAAFQAAACoAAw0AAAAAVQAGGgAAAABgAAAAgAAKJgAAAACqAA0zAAATTAAAGmYAAB1zAAAmmQAAM8wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACgoKCgoKCAsQEBAQEBANDRAQEBAQEAsPEBAQEBAQAAAKCgoKCgoICxAQEBAQEA0NEBAQEBAQCw8QEBAQEBAAAAoKCgoKCggLEBAQEBAQDQ0QEBAQEBALDxAQEBAQEAAACgoKCgoKCAsQEBAQEBANDRAQEBAQEAsPEBAQEBAQAAAKCgoKCgoICxAQEBAQEA0NEBAQEBAQCw8QEBAQEBAAAAoKCgoKCggLEBAQEBAQDQ0QEBAQEBALDxAQEBAQEAAACAgICAgIBwkPDw8PDw8MDA8PDw8PDwkODw8PDw8PAAALCwsLCwsJBAsLCwsLCwYCAwMDAwMDAQkLCwsLCwsAABAQEBAQEA8LEBAQEBAQDQUKCgoKCgoDDxAQEBAQEAAAEBAQEBAQDwsQEBAQEBANBQoKCgoKCgMPEBAQEBAQAAAQEBAQEBAPCxAQEBAQEA0FCgoKCgoKAw8QEBAQEBAAABAQEBAQEA8LEBAQEBAQDQUKCgoKCgoDDxAQEBAQEAAAEBAQEBAQDwsQEBAQEBANBQoKCgoKCgMPEBAQEBAQAAAQEBAQEBAPCxAQEBAQEA0FCgoKCgoKAw8QEBAQEBAAAA0NDQ0NDQwGDQ0NDQ0NCwMFBQUFBQUCDA0NDQ0NDQAADQ0NDQ0NDAIFBQUFBQUDCw0NDQ0NDQYMDQ0NDQ0NAAAQEBAQEBAPAwoKCgoKCgUNEBAQEBAQCw8QEBAQEBAAABAQEBAQEA8DCgoKCgoKBQ0QEBAQEBALDxAQEBAQEAAAEBAQEBAQDwMKCgoKCgoFDRAQEBAQEAsPEBAQEBAQAAAQEBAQEBAPAwoKCgoKCgUNEBAQEBAQCw8QEBAQEBAAABAQEBAQEA8DCgoKCgoKBQ0QEBAQEBALDxAQEBAQEAAAEBAQEBAQDwMKCgoKCgoFDRAQEBAQEAsPEBAQEBAQAAALCwsLCwsJAQMDAwMDAwIGCwsLCwsLBAkLCwsLCwsAAA8PDw8PDw4JDw8PDw8PDAwPDw8PDw8JBwgICAgICAAAEBAQEBAQDwsQEBAQEBANDRAQEBAQEAsICgoKCgoKAAAQEBAQEBAPCxAQEBAQEA0NEBAQEBAQCwgKCgoKCgoAABAQEBAQEA8LEBAQEBAQDQ0QEBAQEBALCAoKCgoKCgAAEBAQEBAQDwsQEBAQEBANDRAQEBAQEAsICgoKCgoKAAAQEBAQEBAPCxAQEBAQEA0NEBAQEBAQCwgKCgoKCgoAABAQEBAQEA8LEBAQEBAQDQ0QEBAQEBALCAoKCgoKCgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA)](https://testgrid.knative.dev/eventing-kafka)
[![Slack](https://img.shields.io/badge/Signup-Knative_Slack-white.svg?logo=slack)](https://slack.knative.dev)
[![Slack](https://img.shields.io/badge/%23eventing-white.svg?logo=slack&color=522a5e)](https://knative.slack.com/archives/C9JP909F0)

This repository contains eventing components using Kafka
as the backing implementation.  It currently consists of a 
[Source](pkg/source/README.md) implementation, and a single
KafkaChannel CRD with two backing Channel implementations
([Consolidated](pkg/channel/consolidated/README.md) &
[Distributed](pkg/channel/distributed/README.md)).


## Nightly Artifacts
```shell script
# Install the Kafka Source
kubectl apply -f https://storage.googleapis.com/knative-nightly/eventing-kafka/latest/source.yaml

# Install the Kafka "Consolidated" Channel
kubectl apply -f https://storage.googleapis.com/knative-nightly/eventing-kafka/latest/channel-consolidated.yaml

# Install the Kafka "Distributed" Channel
kubectl apply -f https://storage.googleapis.com/knative-nightly/eventing-kafka/latest/channel-distributed.yaml
```

---

To learn more about Knative, please visit the
[/docs](https://github.com/knative/docs) repository.

This repo falls under the
[Knative Code of Conduct](https://github.com/knative/community/blob/master/CODE-OF-CONDUCT.md)
