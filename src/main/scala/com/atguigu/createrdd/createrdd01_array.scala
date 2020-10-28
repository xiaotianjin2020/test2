package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-22 19:52 
 */
object createrdd01_array {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    //3、使用parallelize()创建rdd
    val rdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8))
    rdd.foreach(println)
    //使用makeRDD()创建rdd
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8))
    rdd1.foreach(println)
    sc.stop()
  }

}
