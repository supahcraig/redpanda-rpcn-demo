I am a Redpanda SE giving a demo to a prospect.    This demo should not be overly complex and need not get deep into the weeds.  It should fully deploy via docker compose running on my local macbook.


Things needed to be built:

1.  3 broker redpanda cluster including redpanda console and redpanda connect
2.  a postgres container that can talk to/from redpanda
3.  repdanda connect pipelines

The postgres instance exists to allow for SQL style lookups to translate first names into the longer form of that name.  For instance, Bill, Billy, Will, and William should all translate to William.   It's possible this is better done with ollama running locally, I am open to ideas that can be quickly implemnted in a demo capacity.


The RPCN pipelines should look as follows:

MQ pipepline - this will generate randomized json data to simmulate a set of sensors on a manufacturing floor, and send them to a redpanda topic. 

Alerting pipeline - this will consume the topic from the MQ pipeline and identify key values that exceed a given threshold.   Roughly 5% of the stream should exceed and trigger an alert message to an alerts topic.

Enrichment pipeline - this will take a randomized set of input customer data (names, addresses, favorite rock band) and enrich it with the formalized first name (as described above).   The customer is currently doing this with a databse lookup, but a call to an LLM might make for a cooler demo.   The destination for this is another redpanda topic.   The success/failure/confidence % should be appended to the payload.

FTP pipeline that monitors a folder for new files and pushes them into a local object store.   As a stretch goal if we can make this write as iceberg and have something query it (like trino, etc) it would be great.
