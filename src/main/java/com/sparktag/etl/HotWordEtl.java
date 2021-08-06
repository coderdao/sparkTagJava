package com.sparktag.etl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class HotWordEtl {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        // SparkContext context = new SparkContext();

        SparkConf conf = new SparkConf().setAppName("hot-word").setMaster("local[*]")
                .set("dfs.client.use.datanode.hostname", "true")
                .set("spark.hadoop.fs.default.name", "hdfs://192.168.20.157:8020")
                .set("spark.hadoop.fs.defaultFS", "hdfs://192.168.20.157:8020")
                .set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName());

        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> linesRdd = jsc.textFile("hdfs://192.168.20.157:8020/student/SogouQ.sample.txt");
        System.out.println(linesRdd.top(10));

        JavaPairRDD<String, Integer> pairRDD = linesRdd.mapToPair(new PairFunction<String, String, Integer>() {
            // 搜索内容映射成 次数 -> 词语
            // string -- (string, interget)
            public Tuple2<String, Integer> call(String s) throws Exception {
                String word = s.split("\t")[2];
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        // 搜索词 次数迭代
        JavaPairRDD<String, Integer> resultRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                // ("hello", 1),  ("hello", 1)
                return integer+integer2;
            }
        });

//        List<Tuple2<String, Integer>> take = resultRDD.take(10);
//        for (Tuple2<String, Integer> tuple2 : take) {
//            System.out.println(tuple2._1 + "===" + tuple2._2);
//        }
        JavaPairRDD<Integer, String> swapRDD = resultRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            // ("hello", 1) => (1, "hello")
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });

        JavaPairRDD<Integer, String> sorted = swapRDD.sortByKey(false);
        sorted.take(10);
    }
}
