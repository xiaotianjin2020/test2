package com.atguigu.test4

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-25 11:32 
 */
object action01_reduce {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val result1: Int = rdd.aggregate(10)(_+_,_+_)
    println(result1)
   /* val result: Array[Int] = rdd.takeOrdered(2)
    println(result.mkString(","))*/
/*    rdd.collect().foreach(println)*/
/*    val first: Int = rdd.first()
    val ints: Array[Int] = rdd.take(2)
    println(ints)
    ints.foreach(println)*/
/*    val count_sum: Long = rdd.count()
    println(count_sum)*/
   // println(first)
   /* val result: Int = rdd.reduce(_ + _)
    println(result)*/
    //4.关闭连接
    sc.stop()
  }
}
