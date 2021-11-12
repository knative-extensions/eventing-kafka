# KafkaChannel Webhook Config

This Webhook configuration is shared by both KafkaChannel implementations.

**Notes:**

- The `webhook-deployment-ro.yaml` is a copy of `webhook-deployment.yaml`, with
  an additional Environment Variable specifying support for ResetOffsets. It is
  expected users of the webhook will link one or the other of the two
  deployments. Once both KafkaChannel implementations have enabled the
  ResetOffset Controller, we can again consolidate the two copies and remove the
  Environment Variable and decision logic in `webhook/main.go`.

