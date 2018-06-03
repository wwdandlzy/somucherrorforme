package com.bestpay.bdt.dulbank;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import JavaSessionize.avro.KafkaEvent;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import io.confluent.kafka.serializers.KafkaAvroDecoder;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;


public class KafkaCustomerManager implements Serializable{
	private String groupId = null;
	private String topic = null;
	private KafkaCluster kafkaCluster = null;
	private Map<String, String> kafkaParams = new HashMap<String, String>();
	private Map<TopicAndPartition, Long> fromOffsets = new HashMap<TopicAndPartition, Long>();
	// private static final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();  
	
    public KafkaCustomerManager(String _brokerList, String _groupId, String _topic,String schema_registry_url){
		groupId = _groupId;
		topic = _topic;
		kafkaParams.put("metadata.broker.list", _brokerList);
		kafkaParams.put("group.id", groupId);
		kafkaParams.put("schema.registry.url", schema_registry_url);
		kafkaParams.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		kafkaParams.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");



        scala.collection.mutable.Map<String, String> mutableKafkaParam = JavaConversions.mapAsScalaMap(kafkaParams);
        scala.collection.immutable.Map<String, String> immutableKafkaParam = mutableKafkaParam
                .toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
					private static final long serialVersionUID = 1L;
					
					public Tuple2<String, String> apply(
                            Tuple2<String, String> v1) {
                        return v1;
                    }
                });
        this.kafkaCluster = new KafkaCluster(immutableKafkaParam);
    }


    private void initZKOffsets(boolean isSetLatestOffset){
        Set<String> topicSet = new HashSet<String>();
        topicSet.add(topic);
        scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicSet);
        scala.collection.immutable.Set<String> immutableTopics = mutableTopics.toSet();
        System.out.println(this.kafkaCluster.getPartitions(immutableTopics));
        scala.collection.immutable.Set<TopicAndPartition> scalaTopicAndPartitionSet =

               this.kafkaCluster.getPartitions(immutableTopics).right().get();

        //首次消费，从0开始
        if (kafkaCluster.getConsumerOffsets(kafkaParams.get("group.id"), scalaTopicAndPartitionSet).isLeft()) {
            Set<TopicAndPartition> javaTopicAndPartitionSet = JavaConversions.setAsJavaSet(scalaTopicAndPartitionSet);
            for (TopicAndPartition topicAndPartition : javaTopicAndPartitionSet) {
                this.fromOffsets.put(topicAndPartition, 0L);
            }
            System.out.println("### initZKOffsets, set kafka offset to 0");
            
        } else {
            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp =
                    kafkaCluster.getConsumerOffsets(kafkaParams.get("group.id"), scalaTopicAndPartitionSet).right().get();
            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);

            scala.collection.immutable.Map<TopicAndPartition, LeaderOffset> scalaEarliestMap =
                    kafkaCluster.getEarliestLeaderOffsets(scalaTopicAndPartitionSet).right().get();
            Map<TopicAndPartition, LeaderOffset> javaEarliestMap
                    = JavaConversions.mapAsJavaMap(scalaEarliestMap);

            scala.collection.immutable.Map<TopicAndPartition, LeaderOffset> scalaLatestMap =
                    kafkaCluster.getLatestLeaderOffsets(scalaTopicAndPartitionSet).right().get();
            Map<TopicAndPartition, LeaderOffset> javaLatestMap
                    = JavaConversions.mapAsJavaMap(scalaLatestMap);

            Set<TopicAndPartition> javaTopicAndPartitionSet = JavaConversions.setAsJavaSet(scalaTopicAndPartitionSet);
            for (TopicAndPartition topicAndPartition : javaTopicAndPartitionSet) {
                Long zkOffset = (Long) consumerOffsets.get(topicAndPartition);
                long earliestOffset = javaEarliestMap.get(topicAndPartition).offset();
                long latestOffset = javaLatestMap.get(topicAndPartition).offset();
                
                System.out.println("### topic=" + topicAndPartition.topic()  + ", partition=" +
                        topicAndPartition.partition() + ", earliestOffset=" + earliestOffset
                        + ", latestOffset=" + latestOffset + ", zkOffset=" + zkOffset);
                
                if(isSetLatestOffset){
                    zkOffset = latestOffset;
                    // System.out.println("### initZKOffsets, set kafka offset to latestOffset");
                } else if (zkOffset < earliestOffset || zkOffset > latestOffset ){
                	// System.out.println("### initZKOffsets, set kafka offset to latestOffset");
                    zkOffset = latestOffset;
                }

                this.fromOffsets.put(topicAndPartition, zkOffset);
            }
        }
    }


    /**
     * 更新offsets信息
     */
    public void updateZKOffsets(JavaInputDStream<KafkaEvent> stream){
        stream.foreachRDD(new VoidFunction<JavaRDD<KafkaEvent>>() {
			private static final long serialVersionUID = -7190260568004469046L;

			@Override
            public void call(JavaRDD<KafkaEvent> arg0) {
                OffsetRange[] offsets = ((HasOffsetRanges) arg0.rdd()).offsetRanges();
                for(OffsetRange o: offsets){
                    // 封装topic.partition 与 offset 对应关系 java Map
                    TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
                    Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap<TopicAndPartition, Object>();
                    topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());

                    // 转换java map to scala immutable.map
                    scala.collection.mutable.Map<TopicAndPartition, Object> map =
                            JavaConversions.mapAsScalaMap(topicAndPartitionObjectMap);
                    scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap =
                            map.toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
								private static final long serialVersionUID = 1L;
								
								public Tuple2<TopicAndPartition, Object> apply(Tuple2<TopicAndPartition, Object> v1) {
                                    return v1;
                                }
                            });

                    // 更新 offset 到 kafkaCluster
                    try {
                        kafkaCluster.setConsumerOffsets(groupId, scalatopicAndPartitionObjectMap);
                    } catch (Exception e) {
                        System.out.println("### update zk offsets is error, offsets: " + o);
                        System.out.println("### update zk offsets is error: " + e.getStackTrace());
                    }
                }
                
                //System.out.println("###updateZKOffsets, kafkaCluster handle message count:" + arg0.count());
            }
        });
    }


    public JavaInputDStream<KafkaEvent> createDirectStream(JavaStreamingContext jssc, boolean isSetLatestOffset) {
        initZKOffsets(isSetLatestOffset);
        return KafkaUtils.createDirectStream(
                jssc,
                String.class,
                Object.class,
                StringDecoder.class,
                KafkaAvroDecoder.class,
                KafkaEvent.class,
                kafkaParams,
                this.fromOffsets,
                new Function<MessageAndMetadata<String, Object>, KafkaEvent>() {
                    private static final long serialVersionUID = -1422306238864601043L;
                        public KafkaEvent call(MessageAndMetadata<String, Object> v1) throws Exception {
                            GenericRecord data = (GenericRecord ) v1.message();
                            KafkaEvent event = (KafkaEvent) SpecificData.get().deepCopy(KafkaEvent.SCHEMA$, data);
                            return (event);
                    }
                }
        );
    }
}
