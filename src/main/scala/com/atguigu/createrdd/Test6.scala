package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 9:25 
 */
object Test6 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    // 3.1 创建一个RDD
    val distinctRdd: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1))
    distinctRdd.distinct(2).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }

}
