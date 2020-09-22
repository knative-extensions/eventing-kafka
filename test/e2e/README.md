
# E2E Tests

E2E tests verify the eventing-kafka / KafkaChannel implementation sends
events as expected in various scenarios (chained, backing brokers, etc).

## Running E2E Tests

To run these tests manually against a local valid e2e test cluster...

```bash
# Run All E2E Tests Against KafkaChannels
go test -v -tags=e2e -count=1 ./test/e2e/... -channels=messaging.knative.dev/v1beta1:KafkaChannel

# Run A Specific E2E Test Against KafkaChannels
go test -v -tags e2e -count=1 ./test/e2e/... -channels=messaging.knative.dev/v1beta1:KafkaChannel -run TestSingleBinaryEventForChannel
```

> NOTE: Make sure you have built the [test_images](../README.md#test-images)!
