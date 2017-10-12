Clone the project from git

Build the jars using below command
mvn clean 
mvn package

copy the below jars from target directory to kafka lib
elasticsearch-0.0.1-SNAPSHOT.jar
elasticsearch-0.0.1-SNAPSHOT-jar-with-dependencies.jar

Use the configuration file "connect-elasticsearch-sink.properties"