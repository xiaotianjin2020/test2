package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 20:07 
 */
object KeyValue10_cogroup {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    //3.1 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c")))

    //3.2 创建第二个RDD
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1,4),(2,5),(3,6)))
    rdd.cogroup(rdd1).collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }
}
