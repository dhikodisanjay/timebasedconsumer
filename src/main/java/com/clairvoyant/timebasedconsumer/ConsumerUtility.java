package com.clairvoyant.timebasedconsumer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class ConsumerUtility {

	public static KafkaConsumer<String, String> consumer1 = null;
	public static KafkaConsumer<String, String> consumer2 = null;

	static {
		System.out.println("initializing Consumer1 config*****####******");
		String topic = "testing";
		Properties properties1 = new Properties();
		Properties properties2 = new Properties();
		properties1.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties1.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		properties1.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		properties1.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group1");
		consumer1 = new KafkaConsumer<String, String>(properties1);
		// consumer.subscribe(Arrays.asList("test_topic"));
		System.out.println("Consumer1 initialized");

		System.out.println("initializing Consumer2 config***********");
		properties2.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties2.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		properties2.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		properties2.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group2");
		properties2.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumer2 = new KafkaConsumer<String, String>(properties2);

	}

	public static void main(String[] args) throws ParseException {
		Map<TopicPartition, List<Integer>> offsMap = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		/*
		 * Date dt1=sdf.parse("2019-08-0712:26:25:399"); long startTime=dt1.getTime();
		 * 
		 * Date dt2=sdf.parse("2019-08-07T11:38:00:0.0"); long endTime=dt2.getTime();
		 * 
		 * System.out.println(startTime+"startTime"+endTime+ "endTime");
		 */
		// timeBasedConsumer(1565179320, 222222222);
//timeBasedConsumer(Long.parseLong("1565163177069"),Long.parseLong("12121212"));1565172500873	

		offsMap = timeBasedConsumer(Long.parseLong("1565164032066"), Long.parseLong("1565164200657"));
		// offsMap = timeBasedConsumer(Long.parseLong("1565164200657"),
		// Long.parseLong("1565172500873"));
		// offsMap=timeBasedConsumer(Long.parseLong("1565175560573"),Long.parseLong("1565177582328"));
		// offsMap=timeBasedConsumer(Long.parseLong("1565175560573"),Long.parseLong("1565238933485"));

		fetchData(offsMap);
	}

	public static Map<TopicPartition, List<Integer>> timeBasedConsumer(Long startTimestamp, Long endTimestamp) {

		// Get List of Partitions
		List<PartitionInfo> partitionInfos = consumer1.partitionsFor("test_topic");

		// Transform PartitionInfo into Topic Partition

		List<TopicPartition> partitionList = partitionInfos.stream()
				.map(info -> new TopicPartition("test_topic", info.partition())).collect(Collectors.toList());

		consumer1.assign(partitionList);
		partitionList.forEach(p -> System.out.println("partitions for topic " + p));

//getting start timestamp offsets value
		Map<TopicPartition, Long> startpartitionTimestampMap = partitionList.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> startTimestamp));

		Map<TopicPartition, OffsetAndTimestamp> startpartitionOffsetMap = consumer1
				.offsetsForTimes(startpartitionTimestampMap);
		System.out.println("size of startpartitionOffsetMap " + startpartitionOffsetMap.size());

//getting end timestamp offsets value

		Map<TopicPartition, Long> endpartitionTimestampMap = partitionList.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> endTimestamp));

		Map<TopicPartition, OffsetAndTimestamp> endpartitionOffsetMap = consumer1
				.offsetsForTimes(endpartitionTimestampMap);
		System.out.println("size of endpartitionOffsetMap is " + endpartitionOffsetMap.size());

		// storing start and end offset position for specific partitions
		List<Integer> offsets = new ArrayList<Integer>();
		Map<TopicPartition, List<Integer>> offsetMapForRetrieval = new HashMap<TopicPartition, List<Integer>>();

//seeking offset value from start time stamp	
		startpartitionOffsetMap.forEach((tp, offsetAndTimestamp) -> {
			consumer1.seek(tp, offsetAndTimestamp.offset());
			offsets.add((int) offsetAndTimestamp.offset());
			offsetMapForRetrieval.put(tp, offsets);

		});

//seeking offset value from end time stamp
		endpartitionOffsetMap.forEach((tp, offsetAndTimestamp) -> {
			consumer1.seek(tp, offsetAndTimestamp.offset());
			offsets.add((int) offsetAndTimestamp.offset());
			offsetMapForRetrieval.put(tp, offsets);

		});

		for (TopicPartition partition : offsetMapForRetrieval.keySet()) {

			System.out.println(
					"partition " + partition + "\t start offset \t" + offsetMapForRetrieval.get(partition).get(0)
							+ "\t end offset \t" + offsetMapForRetrieval.get(partition).get(1));

		}

		return offsetMapForRetrieval;

	}

	public static void fetchData(Map<TopicPartition, List<Integer>> offsetMapForRetrieval) {
		int startOffset = 0;
		int endOffset = 0;

		consumer2.subscribe(Arrays.asList("test_topic"));
		System.out.println("fetchData() called");

		ConsumerRecords<String, String> records = consumer2.poll(100);

		if (records == null) {
			System.out.println("records are null");
			System.exit(0);
		}

		for (TopicPartition partition : offsetMapForRetrieval.keySet()) {
			startOffset = offsetMapForRetrieval.get(partition).get(0);
			endOffset = offsetMapForRetrieval.get(partition).get(1);
			System.out.println("partition " + partition + " startOffset " + startOffset + "endOffset" + endOffset);
		}
		// System.out.println(offsetMapForRetrieval.size());
		for (ConsumerRecord<String, String> record : records) {

			if (record.offset() >= startOffset && record.offset() <= endOffset) {

				// System.out.println(record.offset());
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

			}

		}

	}

}
