package com.atguigu.createrdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 19:08 
 */
object KeyValue05_foldByKey {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    //3.1 创建第一个RDD
    val list: List[(String, Int)] = List(("a",1),("a",1),("a",1),("b",1),("b",1),("b",1),("b",1),("a",1))
    val rdd = sc.makeRDD(list,2)
   /* rdd.foldByKey(0)(_+_).collect().foreach(println)*/
    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }

}
