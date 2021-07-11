package com.test.spark.govind;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.commons.lang.StringUtils;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Scanner;

import org.apache.spark.api.java.function.Function2;



public class Main {
    public static void main(String[] args) {


        //System.setProperty("hadoop.home.dir", "C:\\Users\\gp\\Desktop\\Sark_file\\spark-2.4.1-bin-hadoop2.7\\bin\\winutils.exe");
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }
        JavaSparkContext sc = new JavaSparkContext(
                master, "basicmap", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> result = rdd.map(
                new Function<Integer, Integer>() {
                    public Integer call(Integer x) {
                        return x * x;
                    }
                });
        System.out.println(StringUtils.join(result.collect(), ","));
        Scanner sc1 =new Scanner(System.in);
        sc1.nextLine();

    }

    public static void wordCount(String inputFile,
                                 String outputFile) throws Exception {

        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);
        // Split up into words.
        //s -> Arrays.asList(s.split(" ")).iterator());
        JavaRDD<String> words = input.flatMap(wordLine -> Arrays.asList(wordLine.split(" ")).iterator());
        // Transform into word and count.
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String x) {
                        return new Tuple2(x, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        });
        // Save the word count back out to a text file, causing evaluation.
        counts.saveAsTextFile(outputFile);

    }
}