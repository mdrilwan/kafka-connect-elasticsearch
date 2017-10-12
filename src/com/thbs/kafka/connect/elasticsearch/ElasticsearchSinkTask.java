package com.thbs.kafka.connect.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

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
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class ElasticsearchSinkTask extends SinkTask {

	private Map<String, String> indexMapping = new HashMap<String, String>();
	private String documentName;
	private Client client;
	private Long bulksize;
	private static String value1;
	private static String dt;
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

                Map<String, String> jsonMap = null;
				try {
					jsonMap = parseLog(record.value().toString());
				} catch (ParserConfigurationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SAXException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
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

	private Map<String, String> parseLog(String log) throws SAXException, IOException, ParserConfigurationException  {
		 
		
		Map<String, String> jsonMap1 = new HashMap<String, String>();
		   DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
	       DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
	       InputSource is = new InputSource();
	       is.setCharacterStream(new StringReader(log));
	       Document doc = dBuilder.parse(is);
	       Element element = doc.getDocumentElement();
	       NamedNodeMap list = element.getAttributes();
	       for(int i =0;i<list.getLength();i++) {
	    	   Node node = list.item(i);
	    	  // System.out.println(node.getNodeName() + "\t" + node.getNodeValue());
	    	   String key = node.getNodeName();
	    	   String value = node.getNodeValue();
	    	   jsonMap1.put(key, value);
	    	   if(key.toLowerCase().equals("datetime")) 
	    	   {
	    		   key = "@timestamp";
	    		   value1 =jsonMap1.get(node.getNodeName());
	    		   
	    		   String date11=value1;
		   	       SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		   	       String dateInString1 = date11;
		      
		   	       try {

		               Date date2 = formatter1.parse(dateInString1);
		          
		               SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

		               //System.out.println(dateFormatter.format(date));
		                dt = dateFormatter.format(date2);
		               
		   	       } catch (ParseException e) {
		               e.printStackTrace();
		   	       }
	    		   
		   	    jsonMap1.put(key,dt);  
	
	    	   }

	       }
	       //jsonMap1.remove("dateTime");
	       jsonMap1.put("message", log);
	       
	       return jsonMap1;
	       

	}
	
	
}