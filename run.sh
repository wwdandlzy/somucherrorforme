export EAGLE_SPARK_ROOT_DIR=/home/hadoop/lubo
EAGLE_SPARK_ROOT_DIR=/home/lzy/
SPARK_HOME=/opt/module/spark-2.1.1-bin-hadoop2.7

LATEST_JAR=/home/hadoop/lubo/eagle-module-spark2/target
CONF_FILE_PATH=${EAGLE_SPARK_ROOT_DIR}/config/spark.conf
APP_SUBMIT_LOG=${EAGLE_SPARK_ROOT_DIR}/comit_log.id
cd ${SPARK_HOME}
echo "current directory:"`pwd`
APP_JAR_HDFS_DIR=/opt/module/jars
OUTPUTLOG=/user/lubo/log/*
# $1 JAR_PATH_TYPE: HDFS,LOCAL
init_env(){
   CLASS_PATH=com.bestpay.monitor.eagle.modules.spark.srclog.srclog

   SPARK_MASTER_URL=$(grep "^[[:space:]]*spark_master_url" "${CONF_FILE_PATH}" | sed -e "s/spark_master_url=//" | tr -d '\r')
   echo "SPARK_MASTER_URL:"${SPARK_MASTER_URL}
   EXECUTOR_MEMORY=$(grep "^[[:space:]]*wcr_executor_memory" "${CONF_FILE_PATH}" | sed -e "s/wcr_executor_memory=//" | tr -d '\r')
   echo "EXECUTOR_MEMORY:"${EXECUTOR_MEMORY}
   EXECUTOR_CORES=$(grep "^[[:space:]]*wcr_executor_cores" "${CONF_FILE_PATH}" | sed -e "s/wcr_executor_cores=//" | tr -d '\r')
   echo "EXECUTOR_CORES:"${EXECUTOR_CORES}

   init_parameters
   APPLICATION_ARGUMENTS=${CONF_KEY_VALUE}
   echo "APPLICATION_ARGUMENTS:"${APPLICATION_ARGUMENTS}

   JAR_PATH_TYPE=$1
   echo "=====JAR_PATH_TYPE:"${JAR_PATH_TYPE}

   if [ ${JAR_PATH_TYPE} == "HDFS" ]; then
       LIBS=`hadoop fs -find ${APP_JAR_HDFS_DIR} -iname "*.jar"|grep -v "eagle-module-spark2" |sort|xargs|sed "s/ /,hdfs\:\/\//g"`
       JAR_LIB="hdfs://"${LIBS}

       #APPLICATION_JAR=hdfs://172.26.7.88:9000${APP_JAR_HDFS_DIR}/eagle-module-spark2-2.0.jar
       APPLICATION_JAR=hdfs://${APP_JAR_HDFS_DIR}/eagle-module-spark2-2.0.jar
   else
       HBASE_LIB=`find ${SPARK_HOME}/ex-jars/hbase -iname "*.jar"|sort|xargs|sed "s/ /,/g"`
       MYSQL_LIB=`find ${SPARK_HOME}/ex-jars/mysql -iname "*.jar"|sort|xargs|sed "s/ /,/g"`
       KAFKA_ZK_LIB=`find ${SPARK_HOME}/ex-jars/kafka-zk -iname "*.jar"|sort|xargs|sed "s/ /,/g"`
       JAR_LIB=${HBASE_LIB},${KAFKA_ZK_LIB},${MYSQL_LIB}

       APPLICATION_JAR=${EAGLE_SPARK_ROOT_DIR}/lib/eagle-module-spark2-2.0.jar
   fi

   echo "====APPLICATION_JAR:"${APPLICATION_JAR}
   echo "====JAR_LIB:"${JAR_LIB}
}

init_parameters(){
   CONF_KEY_VALUE=""
   KEY_TEMP=(kafka_broker kafka_topic_srclog wc_kafka_groupid wc_durations)
   for KEY in ${KEY_TEMP[@]}
   do
      VALUE=$(grep "^[[:space:]]*${KEY}" "${CONF_FILE_PATH}" | sed -e "s/${KEY}=//" | tr -d '\r')
      #echo  "KEY:"${KEY}", VALUE:"${VALUE}
      CONF_KEY_VALUE=${CONF_KEY_VALUE}" "[$KEY][$VALUE]
   done
}

put_jars_to_hdfs(){
   hadoop fs -rm -r ${OUTPUTLOG}
   hadoop fs -rm -r ${APP_JAR_HDFS_DIR}
   hadoop fs -mkdir -p ${APP_JAR_HDFS_DIR}

   hadoop fs -put ${LATEST_JAR}/eagle-module-spark2-2.0.jar ${APP_JAR_HDFS_DIR}
   hadoop fs -put ${SPARK_HOME}/ex-jars/hbase  ${APP_JAR_HDFS_DIR}
   hadoop fs -put ${SPARK_HOME}/ex-jars/kafka-zk  ${APP_JAR_HDFS_DIR}
   hadoop fs -put ${SPARK_HOME}/ex-jars/mysql  ${APP_JAR_HDFS_DIR}

}

submit_yarn_client(){
   kill_app_yarn_cluster
   echo "run on a yarn client"
   init_env LOCAL
   ./bin/spark-submit \
      --class ${CLASS_PATH} \
      --master yarn \
      --deploy-mode client \
      --executor-memory ${EXECUTOR_MEMORY} \
      --num-executors ${EXECUTOR_CORES} \
      --queue root.default \
      --conf spark.streaming.kafka.maxRatePerPartition=2000 \
      --jars ${JAR_LIB} \
      ${APPLICATION_JAR} \
      ${APPLICATION_ARGUMENTS}
}

submit_yarn_cluster(){
   kill_app_yarn_cluster
   echo "run on a yarn cluster"
    init_env HDFS
    echo ${CLASS_PATH}
   ./bin/spark-submit \
      --class ${CLASS_PATH} \
      --master yarn \
      --name com.bestpay.spark_log \
      --deploy-mode cluster \
      --driver-cores  4\
      --driver-memory 2G \
      --executor-cores  3 \
      --executor-memory 8G \
      --num-executors 32 \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --conf spark.streaming.concurrentJobs=8 \
      --conf spark.default.parallelism=1000 \
      --conf spark.yarn.submit.waitAppCompletion=false \
      --conf spark.streaming.kafka.maxRatePerPartition=50000 \
      --conf spark.shuffle.memoryFraction=0.5 \
      --conf spark.storage.memoryFraction=0.3 \
      --conf spark.shuffle.file.buffer=32m \
      --conf spark.reducer.maxSizeInFlight=256m \
      --conf spark.shuffle.io.maxRetries=3 \
      --conf spark.shuffle.io.retryWait=3s \
      --conf spark.shuffle.sort.bypassMergeThreshold=10000 \
      --conf spark.shuffle.consolidateFiles=true \
      --conf spark.executor.extraJavaOptions="-Xmn2G -Xms8G  -XX:MaxPermSize=1024m -XX:+UseConcMarkSweepGC -XX:CMSFullGCsBeforeCompaction=5 -XX:+CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:+CMSScavengeBeforeRemark -XX:+ExplicitGCInvokesConcurrent -XX:SurvivorRatio=2 -XX:InitialCodeCacheSize=512m -XX:ReservedCodeCacheSize=512m -XX:MaxTenuringThreshold=8 -XX:ParallelGCThreads=8 -XX:+CollectGen0First -Xss5m -XX:+AggressiveOpts -XX:+UseBiasedLocking -XX:+UseCodeCacheFlushing"  \
      --jars ${JAR_LIB} \
       ${APPLICATION_JAR} \
       ${APPLICATION_ARGUMENTS} 2>${APP_SUBMIT_LOG}

   echo ">>>>>>>>>>>>>>appId:"$(grep "Submitted application" ${APP_SUBMIT_LOG} |awk '{print $NF}')
   echo "APP_SUBMIT_LOG="${APP_SUBMIT_LOG}
#--conf spark.shuffle.manager=org.apache.spark.shuffle.sort.HashShuffleManager
#                  "hash"?->?"org.apache.spark.shuffle.hash.HashShuffleManager",??
}

kill_app_yarn_cluster(){
   echo "kill app……"
   echo "APP_SUBMIT_LOG="${APP_SUBMIT_LOG}
   appId=$(grep "Submitted application" ${APP_SUBMIT_LOG} |awk '{print $NF}')
   echo "app id:"${appId}
   if [ ! -z ${appId} ]; then
      echo "" >${APP_SUBMIT_LOG}
      yarn application -kill ${appId}
      hadoop fs -rm -r /user/bestpay/.sparkStaging/${appId}
   fi
}

keep_alive_for_yarn_cluster(){
  date
  echo "keep application alive."
  appId=$(grep "Submitted application" ${APP_SUBMIT_LOG} |awk '{print $NF}')
  echo "app id:"${appId}

  restart=N
  if [ ! -z ${appId} ]; then
    restart=Y
    appState=`yarn application -list |grep ${appId} |grep -E "ACCEPTED|RUNNING"`
    echo "application state:${appState}"

    if [ ! -z "${appState}" ]; then
       echo "application is running"
       restart=N
   fi
  fi

  echo ">>>>>>>>restart flag:"${restart}
  if [ ${restart} == "Y" ]; then
     echo "restart application."
     yarn application -kill ${appId}
     hadoop fs -rm -r /user/bestpay/.sparkStaging/${appId}
     submit_yarn_cluster
     echo `date +"%F %T"`" restart application SrcLogWordCountReport">>${EAGLE_SPARK_ROOT_DIR}/logs/shell.log
  fi
}
displayHelp(){
  cat <<EOF
################################### help ##############################################
usage: $0 <opt_type>
opt_type: 00   put jars tohdfs
          20   yarn client
          21   yarn cluster
          22   yarn cluster (kill app)
          23   yarn cluster (keep alive)
####################################################################################
EOF
}
OPT_TYPE=$1
echo ">>>script parameters OPT_TYPE:"${OPT_TYPE}
case "$OPT_TYPE" in
  00)
     put_jars_to_hdfs
  ;;
  20)
     submit_yarn_client
  ;;
  21)
     submit_yarn_cluster
  ;;
  22)
     kill_app_yarn_cluster
  ;;
  23)
    keep_alive_for_yarn_cluster
  ;;
  *)
    displayHelp
    ;;
esac
