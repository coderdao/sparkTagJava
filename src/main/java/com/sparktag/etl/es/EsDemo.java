package com.sparktag.etl.es;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EsDemo {
    public static void main(String[] arge) {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("es_demo")
                .set("es.nodes", "192.168.20.157")
                .set("es.port", "9200")
                .set("es.nodes.wan.only", "true")
                .set("es.index.auto.create", "true"); // 自动创建不存在 index 索引

        JavaSparkContext jsc = new JavaSparkContext(conf);

        /** ============================== 插入数据 ============================== */
        /*
        List<User> list = new ArrayList<>();
        list.add(new User("Jack", 18));
        list.add(new User("Eric", 28));

        // hadoop - es
        JavaRDD<User> userJavaRDD = jsc.parallelize(list);
        JavaEsSpark.saveToEs(userJavaRDD, "/user/_doc");
        */

        /** ============================== 插入数据 ============================== */
        /**
        // 第一种查询
        JavaPairRDD<String, Map<String, Object>> pairRDD = JavaEsSpark.esRDD(jsc, "/user/_doc");
        Map<String, Map<String, Object>> stringMapMap = pairRDD.collectAsMap();
        System.out.println("原始数据 ============ "+stringMapMap);

        // pairRDD 转 user
        JavaRDD<User> rdd = pairRDD.map(new Function<Tuple2<String, Map<String, Object>>, User>() {
            @Override
            public User call(Tuple2<String, Map<String, Object>> v1) throws Exception {
                User user = new User();
                BeanUtils.populate(user, v1._2());

                return user;
            }
        });

        List<User> collect = rdd.collect();
        System.out.println("遍历生成数据 ============ "+collect);
        */


        // dsl 查询
        String queryStrinf = "{\"query\":{\"bool\":{\"should\":[{\"match\":{\"name\":\"Eric\"}},{\"range\":{\"FIELD\":{\"gte\":30,\"lte\":40}}}]}}}";
        JavaEsSpark.esJsonRDD(jsc, "/user/_doc", queryStrinf);
        Map<String, Map<String, Object>> stringMapMap = pairRDD.collectAsMap();
        System.out.println("原始数据 ============ "+stringMapMap);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class User implements Serializable {
        private String name;
        private Integer age;
    }
}
