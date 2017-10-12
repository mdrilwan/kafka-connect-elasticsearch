package com.test.kafka.connect.elasticsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class ElasticsearchSinkConnector extends SinkConnector{

	private String indexes;
	private String topics;
	private String documentName;
	private String clusterName;
	private String hosts;
	private String port;
	private String bulksize;

	@Override
	public ConfigDef config() {
		return null;
	}

	@Override
	public void start(Map<String, String> props) {
		indexes = props.get("elasticsearch.indexes");
		topics = props.get("topics");
		documentName = props.get("elasticsearch.document.name");
		clusterName = props.get("elasticsearch.cluster.name");
		hosts = props.get("elasticsearch.hosts");
		port = props.get("elasticsearch.port");
		bulksize = props.get("elasticsearch.bulksize");
	}

	@Override
	public void stop() {
	}

	@Override
	public Class<? extends Task> taskClass() {
		return ElasticsearchSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		ArrayList<Map<String, String>> configs = new ArrayList<>();
		Map<String, String> config = new HashMap<>();

		config.put("elasticsearch.indexes", indexes);
		config.put("topics", topics);
		config.put("elasticsearch.document.name", documentName);
		config.put("elasticsearch.cluster.name", clusterName);
		config.put("elasticsearch.hosts", hosts);
		config.put("elasticsearch.port", port);
		config.put("elasticsearch.bulksize", bulksize);
		configs.add(config);

		return configs;
	}

	@Override
	public String version() {
		return null;
	}

}