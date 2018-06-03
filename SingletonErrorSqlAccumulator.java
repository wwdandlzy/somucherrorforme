package com.bestpay.bdt.dulbank;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.CollectionAccumulator;

public class SingletonErrorSqlAccumulator {
    private static volatile CollectionAccumulator instance = null;

    public static CollectionAccumulator getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            synchronized (SingletonErrorSqlAccumulator.class) {
                if (instance == null) {
                    instance = jsc.sc().collectionAccumulator("failedSqlExec");
                }
            }
        }
        return instance;
    }

}
