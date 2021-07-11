package com.test.spark.govind;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import java.util.Scanner;

public class SparkAppConf {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
                sparkConf.setMaster("local[2]").set("spark.app.name", "Spark_Conf_Example");
                sparkConf.set("spark.driver.cores","2");

        SparkContext context = new SparkContext(sparkConf);
        System.out.println(context.getConf().toDebugString());


        Scanner sc1 =new Scanner(System.in);
        sc1.nextLine();

        context.stop();
    }
}
