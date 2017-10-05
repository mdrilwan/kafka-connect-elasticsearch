package com.test.kafka.connect.elasticsearch;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ElasticsearchSinkTask extends SinkTask {

	private Map<String, String> indexMapping = new HashMap<String, String>();
	private String documentName;
	private Client client;
	private Long bulksize;

	@Override
	public String version() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void put(Collection<SinkRecord> sinkRecords) {
		List<SinkRecord> records = new ArrayList<SinkRecord>(sinkRecords);

		for (int i = 0; i < records.size(); i++) {
            BulkRequestBuilder bulkRequest = client.prepareBulk();

            for (int j = 0; j < bulksize && i < records.size(); j++, i++) {
            	SinkRecord record = records.get(i);

            	Map<String, String> jsonMap = new HashMap<String, String>();
            	jsonMap.put("message", record.value().toString());
            	String topic = record.topic();
            	StringBuilder index = new StringBuilder()
                        	.append(indexMapping.get(topic));

                bulkRequest.add(client.prepareIndex(index.toString(), documentName, Long.toString(record.kafkaOffset())).setSource(jsonMap));
            }
            i--;
            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                for (BulkItemResponse item : bulkResponse) {
                    System.out.println(item.getFailureMessage());
                }
            }
        }
	}

	@Override
	public void start(Map<String, String> props) {
		String[] index = props.get("elasticsearch.indexes").split(",");
		String[] topics = props.get("topics").split(",");
		documentName = props.get("elasticsearch.document.name");
		Settings settings = Settings.settingsBuilder()
	            .put("cluster.name", props.get("elasticsearch.cluster.name")).build();
		client = TransportClient.builder().settings(settings).build()
				.addTransportAddress(
						new InetSocketTransportAddress(
								new InetSocketAddress(props.get("elasticsearch.hosts"),
										Integer.parseInt(props.get("elasticsearch.port")))));
						
		for(int i=0;i<topics.length;i++)
			indexMapping.put(topics[i], index[i]);
		bulksize = Long.parseLong(props.get("elasticsearch.bulksize"));
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

}