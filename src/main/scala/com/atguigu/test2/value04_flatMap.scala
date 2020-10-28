package com.atguigu.test2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 23:26 
 */
object value04_flatMap {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
  /*  val listRDD=sc.makeRDD(List(List(1,2),List(3,4),List(5,6),List(7)), 2)
    listRDD.flatMap(list=>list).collect().foreach(println)*/
  //3.1 创建第一个RDD
  val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2),("a",4),("b",9)))
    val rdd1: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    rdd1.collect().foreach(println)
    //rdd1.map(t=>(t._1,t._2.sum)).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
