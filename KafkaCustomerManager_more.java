package com.bestpay.monitor.eagle.modules.spark.manager;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

import com.bestpay.kapok.bean.SkyEyeSrcLogVo;
import com.bestpay.kapok.utils.MessageSerializer;
import com.bestpay.monitor.eagle.modules.flume.vo.exsource.BaseFlumeMsgVo;
import com.bestpay.monitor.eagle.modules.utils.SerializeUtil;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

/**
 * Created by OK on 2016/7/29.
 * bin/zkCli.sh -server 172.26.7.89:2181
 * rmr  /consumers/sparkSrclogWordCount
 */
public class KafkaCustomerManager implements Serializable{
	private static final long serialVersionUID = -6757736629848288642L;
	
	private String groupId = "sparkSrclogWordCount";
	private String topic = "topic-eagle-srclog-from-flume";
	private KafkaCluster kafkaCluster = null;
	private Map<String, String> kafkaParams = new HashMap<String, String>();
	private Map<TopicAndPartition, Long> fromOffsets = new HashMap<TopicAndPartition, Long>();
	// private static final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();  
	
    public KafkaCustomerManager(String _brokerList, String _groupId, String _topic){
		groupId = _groupId;
		topic = _topic;
		kafkaParams.put("metadata.broker.list", _brokerList);
		kafkaParams.put("group.id", groupId);

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

        scala.collection.immutable.Set<TopicAndPartition> scalaTopicAndPartitionSet =
                kafkaCluster.getPartitions(immutableTopics).right().get();

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

            scala.collection.immutable.Map<kafka.common.TopicAndPartition, KafkaCluster.LeaderOffset> scalaEarliestMap =
                    kafkaCluster.getEarliestLeaderOffsets(scalaTopicAndPartitionSet).right().get();
            Map<kafka.common.TopicAndPartition, KafkaCluster.LeaderOffset> javaEarliestMap
                    = JavaConversions.mapAsJavaMap(scalaEarliestMap);

            scala.collection.immutable.Map<kafka.common.TopicAndPartition, KafkaCluster.LeaderOffset> scalaLatestMap =
                    kafkaCluster.getLatestLeaderOffsets(scalaTopicAndPartitionSet).right().get();
            Map<kafka.common.TopicAndPartition, KafkaCluster.LeaderOffset> javaLatestMap
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

    
	public JavaInputDStream<byte[]> createDirectStreamBytes(JavaStreamingContext jssc, boolean isSetLatestOffset) {
		initZKOffsets(isSetLatestOffset);
		return KafkaUtils.createDirectStream(
				jssc, 
				String.class, 
				byte[].class, 
				StringDecoder.class,
				DefaultDecoder.class, 
				byte[].class, 
				kafkaParams, 
				this.fromOffsets,
				new Function<MessageAndMetadata<String, byte[]>, byte[]>() {
					private static final long serialVersionUID = -5935095641211853621L;
					public byte[] call(MessageAndMetadata<String, byte[]> v1) throws Exception {
						return v1.message();
					}
				});
	}
    
    public JavaInputDStream<BaseFlumeMsgVo> createDirectStream(JavaStreamingContext jssc, boolean isSetLatestOffset) {
        initZKOffsets(isSetLatestOffset);
        return KafkaUtils.createDirectStream(
            jssc,
            String.class,
            byte[].class,
            StringDecoder.class,
            DefaultDecoder.class,
            BaseFlumeMsgVo.class,
            kafkaParams,
            this.fromOffsets,
            new Function<MessageAndMetadata<String, byte[]>, BaseFlumeMsgVo>() {
				private static final long serialVersionUID = -1422306238864601043L;
				public BaseFlumeMsgVo call(MessageAndMetadata<String, byte[]> v1) throws Exception {
                    return (BaseFlumeMsgVo) SerializeUtil.unserialize(v1.message());
                }
            }
        );
    }
    

    @SuppressWarnings("unchecked")
	public JavaInputDStream<SkyEyeSrcLogVo> createDirectStreamSky(JavaStreamingContext jssc, boolean isSetLatestOffset) {
        initZKOffsets(isSetLatestOffset);
        return KafkaUtils.createDirectStream(
            jssc,
            String.class,
            Object.class,
            StringDecoder.class,
            MessageSerializer.class,
            SkyEyeSrcLogVo.class,
            kafkaParams,
            this.fromOffsets,
            new Function<MessageAndMetadata<String, Object>, SkyEyeSrcLogVo>() {
				private static final long serialVersionUID = -1422306238864601043L;
				public SkyEyeSrcLogVo call(MessageAndMetadata<String, Object> v1) throws Exception {
                    return (SkyEyeSrcLogVo) v1.message();
                }
            }
        );
    }
    
	public JavaInputDStream<String> createDirectStreamJson(JavaStreamingContext jssc, boolean isSetLatestOffset) {
		initZKOffsets(isSetLatestOffset);
		return KafkaUtils.createDirectStream(
				jssc, 
				String.class, 
				String.class, 
				StringDecoder.class, 	
				StringDecoder.class,	// MessageSerializer.class
				String.class, 
				kafkaParams, 
				this.fromOffsets,
				new Function<MessageAndMetadata<String, String>, String>() {
					private static final long serialVersionUID = 2397321954270198414L;
					
					public String call(MessageAndMetadata<String, String> v1) throws Exception {
						return v1.message();
					}
				});
	}
    
    /**
     * 更新offsets信息
     */
    public void updateZKOffsets(JavaInputDStream<BaseFlumeMsgVo> stream){
        stream.foreachRDD(new VoidFunction<JavaRDD<BaseFlumeMsgVo>>() {
			private static final long serialVersionUID = -7190260568004469046L;

			@Override
            public void call(JavaRDD<BaseFlumeMsgVo> arg0) {
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

    public void updateZKOffsetsSky(JavaInputDStream<SkyEyeSrcLogVo> stream){
        stream.foreachRDD(new VoidFunction<JavaRDD<SkyEyeSrcLogVo>>() {
			private static final long serialVersionUID = -3299076842881037765L;

			@Override
            public void call(JavaRDD<SkyEyeSrcLogVo> arg0) {
				
                OffsetRange[] offsets = ((HasOffsetRanges) arg0.rdd()).offsetRanges();
                for(OffsetRange o: offsets){
                    // 封装topic.partition 与  offset 对应关系 java Map
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
                
                // System.out.println("###updateZKOffsets, kafkaCluster handle message count:" + arg0.count());
				
            }
        });
    }
    
    public void updateZKOffsetsJson(JavaInputDStream<String> stream){
        stream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = -5468249116900992814L;

			@Override
            public void call(JavaRDD<String> arg0) {
				if(!arg0.isEmpty()) {
	                OffsetRange[] offsets = ((HasOffsetRanges) arg0.rdd()).offsetRanges();
	                for(OffsetRange o: offsets){
	                    // 封装 topic.partition 与 offset 对应关系 java Map
	                    TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
	                    Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap<TopicAndPartition, Object>();
	                    topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());
	                    
	                    // 转换 java map to scala immutable.map
	                    scala.collection.mutable.Map<TopicAndPartition, Object> map =
	                            JavaConversions.mapAsScalaMap(topicAndPartitionObjectMap);
	                    scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap =
	                            map.toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
	                            	private static final long serialVersionUID = -7090260568014469046L;
	                            	
									public Tuple2<TopicAndPartition, Object> apply(Tuple2<TopicAndPartition, Object> v1) {
	                                    return v1;
	                                }
	                            });
	
	                    // 更新 offset 到 kafkaCluster
	                    try {
	                        kafkaCluster.setConsumerOffsets(groupId, scalatopicAndPartitionObjectMap);
	                    } catch (Exception e) {
	                        System.out.println("### update zk offsets is error, offsets:" + o);
	                        System.out.println("### update zk offsets is error:" + e.getStackTrace());
	                    }
	                }
	                
	                // System.out.println("### updateZKOffsets, kafkaCluster handle message count: " + arg0.count());
	            } else {
	            	System.err.println("### ERROR: updateZKOffsetsJson, JavaRDD<String> arg0.isEmpty()......");
				}
        }
        });
    }
    
    
    
    /**
     * 获取kafka offset
     * 
     * @return
     */
    public Map<TopicAndPartition, Long> getOffset() {
       Map<TopicAndPartition, Long> fromOffsets = new HashMap<TopicAndPartition, Long>();
       Set<String> topicSet = new HashSet<String>();
       topicSet.add(topic);
       scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicSet);
       scala.collection.immutable.Set<String> immutableTopics = mutableTopics.toSet();
       scala.collection.immutable.Set<TopicAndPartition> scalaTopicAndPartitionSet = kafkaCluster
             .getPartitions(immutableTopics).right().get();

       // 首次消费
       if (kafkaCluster.getConsumerOffsets(kafkaParams.get("group.id"), scalaTopicAndPartitionSet)
             .isLeft()) {
          scala.collection.immutable.Map<TopicAndPartition, LeaderOffset> earliestOffsetsTemp = kafkaCluster
                .getEarliestLeaderOffsets(scalaTopicAndPartitionSet).right().get();
          Set<TopicAndPartition> javaTopicAndPartitionSet = JavaConversions.setAsJavaSet(scalaTopicAndPartitionSet);
          Map<TopicAndPartition, LeaderOffset> earliestOffsets = JavaConversions.mapAsJavaMap(earliestOffsetsTemp);
          for (TopicAndPartition topicAndPartition : javaTopicAndPartitionSet) {
             LeaderOffset latestOffset = earliestOffsets.get(topicAndPartition);
             fromOffsets.put(topicAndPartition, latestOffset.offset());
          }
       } else {
          scala.collection.immutable.Map<TopicAndPartition, LeaderOffset> earliestOffsetsTemp = kafkaCluster
                .getEarliestLeaderOffsets(scalaTopicAndPartitionSet).right().get();
          scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp = kafkaCluster
                .getConsumerOffsets(kafkaParams.get("group.id"), scalaTopicAndPartitionSet)
                .right().get();
          Map<TopicAndPartition, LeaderOffset> earliestOffsets = JavaConversions.mapAsJavaMap(earliestOffsetsTemp);
          Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);
          Set<TopicAndPartition> javaTopicAndPartitionSet = JavaConversions.setAsJavaSet(scalaTopicAndPartitionSet);
          for (TopicAndPartition topicAndPartition : javaTopicAndPartitionSet) {
             LeaderOffset earliestOffset = earliestOffsets.get(topicAndPartition);
             Long offset = (Long) consumerOffsets.get(topicAndPartition);
             // 如果消费的offset小于leader的earlistOffset，有可能是kafka定时清理已删除该offset文件
             // 这时将过期的offset更新为leader的earlistOffset开始消费，避免offsetOutOfRang异常
             if (offset < earliestOffset.offset()) {
                offset = earliestOffset.offset();
             }
             fromOffsets.put(topicAndPartition, offset);
          }
       }
       return fromOffsets;
    }
    
    /**
     * 设置kafka offset
     * 
     * @param range
     */
    public void setOffset(HasOffsetRanges range) {
       OffsetRange[] offsets = range.offsetRanges();
       for (OffsetRange o : offsets) {
          // 封装topic.partition 与 offset对应关系 java Map
          TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
          Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap<TopicAndPartition, Object>();
          topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());
          
          // 转换java map to scala immutable.map
          scala.collection.mutable.Map<TopicAndPartition, Object> map = JavaConversions
                .mapAsScalaMap(topicAndPartitionObjectMap);
          scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap = map.toMap(
                new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
                   private static final long serialVersionUID = 1L;
                   
                   public Tuple2<TopicAndPartition, Object> apply(Tuple2<TopicAndPartition, Object> v1) {
                      return v1;
                   }
                });

          // 更新offset到kafkaCluster
          kafkaCluster.setConsumerOffsets(kafkaParams.get("group.id"), scalatopicAndPartitionObjectMap);
       }
    }
    
}
