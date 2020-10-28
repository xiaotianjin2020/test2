package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 18:43 
 */
object KeyValue02_reduceByKey {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    //3.1 创建第一个RDD
    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)))
    val rdd1: RDD[(String, Int)] = rdd.reduceByKey(_+_)
    rdd1.collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }
}
