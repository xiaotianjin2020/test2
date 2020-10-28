package com.atguigu.test2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 23:08 
 */
object value03_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)
    rdd.mapPartitionsWithIndex((index,items)=>items.map((index,_))).collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }
}
