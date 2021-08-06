package com.sparktag.etl.es;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class EsDemo {
    public static void main(String[] arge) {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("es_demo")
                .set("es.nodes", "namenode")
                .set("es.index.auto.create", "true"); // 自动创建不存在 index 索引

        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<User> list = new ArrayList<>();
        list.add(new User("Jack", 18));
        list.add(new User("Eric", 28));

        JavaRDD<User> userJavaRDD = jsc.parallelize(list);
        JavaEsSpark.saveToEs(userJavaRDD, "/user/_doc");
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class User implements Serializable {
        private String name;
        private Integer age;
    }
}
