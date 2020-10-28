package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 16:35 
 */
object Test8 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    //4. 创建一个RDD
    // val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 3)
    //合并分区  不执行shuffle
    //val rdd1: RDD[Int] = rdd.coalesce(2)
    /* val rdd2: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex((index,items)=>items.map((index,_)))
     rdd2.collect().foreach(println)
     val rdd3: RDD[Int] = rdd.coalesce(2,true)
     rdd3.mapPartitionsWithIndex((index,items)=>items.map((index,_))).collect().foreach(println)*/
    //val rdd4: RDD[Int] = rdd.repartition(2)
    //rdd4.mapPartitionsWithIndex((index,items)=>items.map((index,_))).collect().foreach(println)
    //rdd.sortBy(num=>num).collect().foreach(println)
    //rdd.sortBy(num=>num,false).collect().foreach(println)
    //val rdd1: RDD[Int] = sc.makeRDD(1 to 4)
    //val rdd2: RDD[Int] = sc.makeRDD(4 to 8)
    // rdd1.intersection(rdd2).collect().foreach(println)
    //rdd1.union(rdd2).collect().foreach(println)
  /*  val rdd1: RDD[Int] = sc.makeRDD(Array(1, 2, 3), 3)
    val rdd2: RDD[String] = sc.makeRDD(Array("a", "b", "c"), 3)
    rdd1.zip(rdd2).collect().foreach(println)*/
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc")),3)
    val rdd3: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))
    rdd3.mapPartitionsWithIndex((index,items)=>items.map((index,_))).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
