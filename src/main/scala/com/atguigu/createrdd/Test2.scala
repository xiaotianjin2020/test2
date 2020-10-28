package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 9:06 
 */
object Test2 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)
    //创建一个2个分区的RDD，并将每个分区的数据放到一个数组，求出每个分区的最大值
   rdd.glom().map(_.max).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }

}
