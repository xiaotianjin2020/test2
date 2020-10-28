package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 8:58 
 */
object Test1 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    //创建一个RDD，使每个元素跟所在分区号形成一个元组，组成一个新的RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)
    val rddtest1: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index,items)=>items.map((index,_)))
    rddtest1.collect().foreach(println)
   //4.关闭连接
    sc.stop()
  }

}
