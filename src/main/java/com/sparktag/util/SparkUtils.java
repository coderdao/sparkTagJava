package com.sparktag.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {

    private static ThreadLocal<JavaSparkContext> jscPool = new ThreadLocal<>();
    private static ThreadLocal<SparkSession> sessionPool = new ThreadLocal<>();

    /**
     * 获取 jsc
     *
     * @return
     */
    public static JavaSparkContext getJSC4Es(Boolean auto) {
        JavaSparkContext javaSparkContext = jscPool.get();
        if (javaSparkContext != null) {
            return javaSparkContext;
        }
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("es demo");
        conf.set("es.nodes", "namenode");
        conf.set("es.port", "9200");
        conf.set("es.index.auto.create", auto.toString());
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jscPool.set(jsc);
        return jsc;
    }

    public static SparkSession initSession() {
        if (sessionPool.get() != null) {
            return sessionPool.get();
        }
/*

        SparkSession session = SparkSession.builder()
                .appName("member etl")
                .master("local[*]")
                .config("es.nodes", "namenode")
                .config("es.port", "9200")
                .config("es.index.auto.create", "false")
                .enableHiveSupport()
                .getOrCreate();
*/

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("member_etl")
                .set("es.nodes", "192.168.20.157")
                .set("es.port", "9200")
                .set("es.nodes.wan.only", "true")
                .set("es.index.auto.create", "true"); // 自动创建不存在 index 索引

        SparkSession session = SparkSession.builder()
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        sessionPool.set(session);
        return session;

    }


}
