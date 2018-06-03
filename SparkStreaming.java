package com.bestpay.bdt.dulbank;

import JavaSessionize.avro.KafkaEvent;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.CollectionAccumulator;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class SparkStreaming {

    private static String mysql_url = "";//"jdbc:mysql://hadoop-cdh-06:3306/bank?characterEncoding=utf8&useSSL=true";
    private static String mysql_username = "";//"root";
    private static String mysql_password = "";//"000000";
    private static String kafka_broker = "";//"hadoop-cdh-05:9092,hadoop-cdh-06:9092,hadoop-cdh-07:9092";
    private static String kafka_topic = "";//"table1";
    private static String kafka_groupid = "";//"wwd";
    private static String schame_registry_url = "";//"http://hadoop-cdh-05:8081";
    private static final Log driverEndPointLogger = LogFactory.getLog("errorSqlInDriver");
    private static final Log executorEndPointLogger = LogFactory.getLog("errorSqlInExecutor");

    public static void main(String... args) throws InterruptedException {
        Map<String, String> argMap = new HashMap<String, String>();
        for (String arg : args) {
            String[] pops = StringUtils.substringsBetween(arg, "[", "]");
            argMap.put(pops[0], pops[1]);
            System.out.println("===>>>parameters, key:" + pops[0] + ", value:" + pops[1]);
        }
        mysql_url = argMap.get("mysql_url");
        mysql_username = argMap.get("mysql_username");
        mysql_password = argMap.get("mysql_password");
        kafka_broker = argMap.get("kafka_broker");
        kafka_topic = argMap.get("kafka_topic");
        kafka_groupid = argMap.get("kafka_groupid");
        schame_registry_url = argMap.get("schame_registry_url");

        SparkConf conf = new SparkConf();
        conf.setMaster("local[1]");
        conf.setAppName("Spark Streaming Test Java");
        conf.registerKryoClasses(new Class[]{KafkaEvent.class});

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10));

        runCheckLog(sc);
        processStream(ssc, sc);
        ssc.start();
        ssc.awaitTermination();
    }

    private static void processStream(JavaStreamingContext ssc, JavaSparkContext sc) {
        JavaSparkContext thissc = sc;
        //InitzkOffset
        KafkaCustomerManager kafkaCustomerManager = new KafkaCustomerManager(kafka_broker, kafka_groupid, kafka_topic, schame_registry_url);
        JavaInputDStream<KafkaEvent> stream = kafkaCustomerManager.createDirectStream(ssc, true);


        stream.foreachRDD(rdd -> {
            CollectionAccumulator collectionAccumulator = SingletonErrorSqlAccumulator.getInstance(thissc);
            rdd.foreachPartition(iterator -> {
                        Connection connTask = ConnectMysql.getConnection(mysql_url, mysql_username, mysql_password);
                        while (iterator.hasNext()) {
                            KafkaEvent model = iterator.next();
                            String sql = createSql(model);
                            if (sql.trim().length() > 0) {
                                try {
                                    Statement statement = connTask.createStatement();
                                    int result = statement.executeUpdate(sql);
                                    if (result == 0) {
                                        collectionAccumulator.add(sql);
                                    }
                                    statement.close();
                                } catch (Exception e) {
                                    executorEndPointLogger.error(sql + "\r\n");
                                }
                            }
                        }
                        ConnectMysql.close(connTask);
                    }
            );
        });
        //更新offset
        kafkaCustomerManager.updateZKOffsets(stream);
    }

    private static void runCheckLog(JavaSparkContext sc) {
        //在driver中启动一个线程,定期将插入数据库失败的sql重新执行一次
        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        this.sleep(10000);
                        CollectionAccumulator accumulator = SingletonErrorSqlAccumulator.getInstance(sc);
                        Iterator iteratorSql = accumulator.value().iterator();
                        accumulator.reset();
                        Connection conn = ConnectMysql.getConnection(mysql_url, mysql_username, mysql_password);
                        while (iteratorSql.hasNext()) {
                            String sql = iteratorSql.next().toString();
                            Statement statement = conn.createStatement();
                            if (statement.executeUpdate(sql) == 0)
                                driverEndPointLogger.error(sql + "\r\n");
                            statement.close();
                        }

                        ConnectMysql.close(conn);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();
    }

    private static String createSql(KafkaEvent model) {
        String sql = "";
        Map<CharSequence, CharSequence> additional = model.getColumns();
        Set<CharSequence> keySet = additional.keySet();
        StringBuffer colnumes = new StringBuffer("");
        StringBuffer values = new StringBuffer("");
        String whereStr = model.getWhere().toString();
        if ("insert".equals(model.getActionType().toString())) {
            for (CharSequence colume : keySet) {
                if (!colume.toString().equals("id")) {
                    String value = getColumnValue(additional.get(colume));
                    if (value != null) {
                        values.append(value + ",");
                        colnumes.append(converColumn(colume.toString()) + ",");
                    }
                }
            }
            if (colnumes.length() != 0) {
                sql = "insert into " + model.getTableName().toString() + "(" + colnumes.toString().substring(0, colnumes.length() - 1) + ")" + " values(" + values.toString().substring(0, values.length() - 1) + ")";
            }
        } else if ("update".equals(model.getActionType().toString())) {
            for (CharSequence colume : keySet) {
                if (!"id".equals(colume.toString())) {
                    String value = getColumnValue(additional.get(colume));
                    if (value != null) {
                        values.append(converColumn(colume.toString()) + "=" + value + ",");
                    }
                }
            }
            if (whereStr.trim().length() != 0 || values.length() != 0) {
                sql = "update " + model.getTableName().toString() + " set " + values.toString().substring(0, values.length() - 1) + " where " + whereStr;
                System.out.println(sql);
            }
        }
        return sql;
    }

    private static String getColumnValue(CharSequence charSequence) {
        String value = null;
        String[] typeAndValue = charSequence.toString().split(":");
        if (typeAndValue.length == 1 || typeAndValue[1].trim().isEmpty() || "".equals(typeAndValue[1].trim()))
            return null;
        if ("String".equals(typeAndValue[0]) || "Date".equals(typeAndValue[0]) || "Character".equals(typeAndValue[0]))
            value = "'" + typeAndValue[1] + "'";
        else
            value = typeAndValue[1];
        return value;
    }

    public static String converColumn(String name) {
        StringBuilder result = new StringBuilder();
        // 循环处理字符
        for (int i = 0; i < name.length(); i++) {
            String s = name.substring(i, i + 1);
            // 在大写字母前添加下划线
            if (s.equals(s.toUpperCase()) && !Character.isDigit(s.charAt(0))) {
                result.append("_");
            }
            // 其他字符直接转成大写
            result.append(s.toLowerCase());
        }

        return result.toString();
    }


}
