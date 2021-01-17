# SignalK Batcher

A SignalK helper for plugins that periodically do frequent full retrieves on a
schedule, then perodically operate on the most recent snapshots in a batch
fashion.  An example of this is [signalk-to-batch-format], which periodically
produces a batch json file locally, and optionally uploads to S3.

See [signalk-to-batch-format] for example usage.

[signalk-to-batch-format]: https://github.com/c33howard/signalk-to-batch-format
