You can start using kafka connect for elasticsearch sink (version 2.x) by following the below steps.

Step 1 : Clone the project

Step 2 : Create directory lib

Step 3 : Download the required libraries from https://drive.google.com/drive/folders/0ByruaDAVhDDPVGU2MFJoQ1dpbEk and place it in lib folder

Step 4 : Compile and create jar file by using the below command

javac -cp .:lib/elasticsearch-2.3.4.jar:lib/kafka-clients-0.10.2.1.jar:lib/connect-api-0.10.2.1.jar src/com/test/kafka/connect/elasticsearch/ElasticsearchSink*.java

cd src

jar -cvf connect-elasticsearch-sink com/

Step 5: Use the below configuration file - connect-elasticsearch-sink.properties

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
