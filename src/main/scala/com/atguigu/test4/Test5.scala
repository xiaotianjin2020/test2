package com.atguigu.test4

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-27 8:57 
 */
object Test5 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("input/agent.log")
    val prvRDD: RDD[(String, Int)] = rdd.map {
      line => {
        val data: Array[String] = line.split(" ")
        (data(1) + "-" + data(4), 1)
      }
    }
    //第二步进行分组聚合  以 prv +次数
    val rdd2: RDD[(String, Int)] = prvRDD.reduceByKey(_+_)
    //第三步 转化格式 辽宁,(AAA,1)
    val rdd3: RDD[(String, (String, Int))] = rdd2.map {
      case (prv, index) => {
        val prvdata: Array[String] = prv.split("-")
        (prvdata(0), (prvdata(1), index))
      }
    }
    //第四步  以省份为key 返回 省份  一个迭代器元组
    val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.groupByKey()
    //第五步 取出value值，是个迭代器 转化成list，对元组内部进行排序
    val rdd5: RDD[(String, List[(String, Int)])] = rdd4.mapValues({
      items =>
        items.toList.sortWith(
          (left, Right) => left._2 > Right._2
        ).take(3)
    })
    rdd5.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }


}
