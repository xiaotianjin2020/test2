package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 9:18 
 */
object Test4 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    //3.创建一个RDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4),2)
/*    rdd.filter(_%2==0).collect().foreach(println)*/
    rdd.sortBy(num=>num).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }

}
