Steps:
Clone the project
Build the jar file
Use the below configuration file:

    connect-elasticsearch-sink.properties:
        name=elasticsearch-schema-sink
        connector.class=com.test.kafka.connect.elasticsearch.ElasticsearchSinkConnector
        tasks.max=1
        topics=textData
        elasticsearch.indexes=textdata
        elasticsearch.cluster.name=elasticsearch
        elasticsearch.hosts=localhost
        elasticsearch.port=9300
        elasticsearch.bulksize=10
        elasticsearch.document.name=document
