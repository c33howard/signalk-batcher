# SignalK to Batch Points

A SignalK helper for plugins that periodically do frequent full retrieves on a
schedule, then perodically operate on the most recent snapshots in a batch
fashion.  An example of this is signalk-to-csv, which periodically produces a
csv file locally, or uploads to S3.

See signalk-to-csv for example usage.
