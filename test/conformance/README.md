# Conformance Tests
Conformance tests verify the eventing-kafka / KafkaChannel implementation follows the 
[Knative Eventing API Specifications](https://github.com/knative/eventing/tree/master/docs/spec).

## Running Conformance Tests
To run these tests manually against a local valid e2e test cluster...
```bash
# Run All Conformance Tests Against KafkaChannels
go test -v -tags=e2e -count=1 ./test/conformance/... -channels=messaging.knative.dev/v1beta1:KafkaChannel

# Run A Specific Conformance Test Against KafkaChannels
go test -v -tags e2e -count=1 ./test/conformance/... -channels=messaging.knative.dev/v1beta1:KafkaChannel -run TestChannelStatus
```
> NOTE: Make sure you have built the [test_images](../README.md#test-images)!
