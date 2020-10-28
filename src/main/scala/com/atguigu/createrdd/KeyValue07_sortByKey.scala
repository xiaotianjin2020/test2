package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 19:58 
 */
object KeyValue07_sortByKey {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    /*rdd.sortByKey(true).collect().foreach(println)*/
   /* for (elem <- rdd.sortByKey(false).collect()) {println}*/
    rdd.sortByKey(false).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }

}
