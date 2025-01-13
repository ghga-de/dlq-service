<!-- Please provide a short overview of the features of this service. -->

The DLQ Service provides a way to manage Kafka topics designated as dead letter queues
via a RESTful API interface.

The DLQ Service requires configuration to define the topics and microservices for each
individual DLQ. It will automatically derive the DLQ topic names based on the
configuration and create a dedicated Kafka consumer for each DLQ topic.
