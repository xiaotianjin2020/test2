package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-23 10:51 
 */
object value08_filter {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
 /*   val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4),2)
    val rdd1: RDD[Int] = rdd.filter(_%2==0)*/
 val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))
   // val rdd1: RDD[Int] = dataRDD.sample(false,0.5)

    val rdd1: RDD[Int] = dataRDD.sample(true,2)
    rdd1.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }

}
