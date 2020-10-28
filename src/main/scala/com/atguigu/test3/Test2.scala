package com.atguigu.test3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-25 8:45 
 */
object Test2 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val dataRDD: RDD[String] = sc.textFile("input/agent.log")
    //第一步转换数据结构
    val dataRdd1: RDD[(String, Int)] = dataRDD.map {
      line => {
        val datas: Array[String] = line.split(" ")
        (datas(1) + "-" + datas(4), 1)
      }
    }
    //第二步按key 求聚合
    val dataRDD2: RDD[(String, Int)] = dataRdd1.reduceByKey(_+_)
    //第三步转换数据结构
    val dataRDD3: RDD[(String, (String, Int))] = dataRDD2.map {
      case (prv, index) => {
        val data3: Array[String] = prv.split("-")
        (data3(0), (data3(1), index))
      }
    }
   //第四步按key 分组 groupbykey  返回是 key，和迭代器(里面是元组类型)
    val dataRDD4: RDD[(String, Iterable[(String, Int)])] = dataRDD3.groupByKey()
    val dataRDD5: RDD[(String, List[(String, Int)])] = dataRDD4.mapValues(
      datas => datas.toList.sortWith((left, Right) => left._2 > Right._2).take(3)
    )
    dataRDD5.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }

}
