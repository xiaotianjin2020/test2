package com.atguigu.createrdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 9:14 
 */
object Test3 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(1 to 4, 2)
    //创建一个RDD，按照元素模以2的值进行分组
    rdd.groupBy(_%2).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }

}
