package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-23 11:05 
 */
object value10_distinct {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val rddArray: RDD[String] = sc.makeRDD(Array("a", "b", "c", "d"), 2)
    val resultRdd: RDD[(Int, String)] = rddArray.mapPartitionsWithIndex((index, items) => items.map((index, _))).reduceByKey(_ + _)
    val rdd2Result: RDD[String] = resultRdd.map(_._2)
    rdd2Result.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
