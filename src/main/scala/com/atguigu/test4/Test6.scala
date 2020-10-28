package com.atguigu.test4

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayOps

/**
 * @author shkstart 
 * @create 2020-09-27 11:25 
 */
object Test6 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")
    //第一步 先炸开
    val rdd1: RDD[String] = rdd.flatMap(
      line => {
        val data: ArrayOps.ofRef[String] = line.split("_")
        val array=new Array[String](3)
        var str=""
        str+=data(6)+data(7)+data(8)
        data
      }
    )
    //第二步取值 并转换格式




    //4.关闭连接
    sc.stop()
  }

}
