<!-- Please provide a short overview of the features of this service. -->

The DLQ Service provides a way to manage Kafka topics designated as dead letter queues
via a RESTful API interface.

The DLQ Service subscribes to the configured DLQ topic and saves all inbound events
to the database. Using the REST API to interact with a given service + topic,
events can be previewed, discarded, modified, and requeued for the original service
to re-consume them. When requeuing an event, the DLQ service publishes the event to
a Kafka "retry" topic, whose name is automatically derived from the service name
included in the event's DLQ information.
