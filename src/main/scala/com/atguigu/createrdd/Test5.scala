package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 9:21 
 */
object Test5 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    //3.1 创建一个RDD
    val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))
    //创建一个RDD（1-10），从中选择放回和不放回抽样
    dataRDD.sample(false,0.5).collect().foreach(println)
    dataRDD.sample(true,2).collect().foreach(println)



    //4.关闭连接
    sc.stop()
  }

}
