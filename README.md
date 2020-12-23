Kafka Connect SMT to take string values from a Kafka header and move to key or value)

Properties:

Example on how to add to your connector:
```
transforms=insertuuid
transforms.insertuuid.type=com.github.dhoward1111.kafka.connect.smt.InsertUuid$Value
transforms.insertuuid.uuid.field.name="uuid"
```

Lots borrowed from the Apache Kafka® `InsertField` SMT