package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 9:28 
 */
object Test7 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    // 3.1 创建一个RDD
    val strList: List[String] = List("Hello Scala", "Hello Spark", "Hello World")
    val rdd = sc.makeRDD(strList)
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = rdd.flatMap(_.split(" ")).map((_,1)).groupBy(_._1)
    groupRdd.map(t=>(t._1,t._2.size)).collect().foreach(println)
    //4.关闭连接
    sc.stop()

  }

}
