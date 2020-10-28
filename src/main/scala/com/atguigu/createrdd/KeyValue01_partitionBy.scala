package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 18:32 
 */
object KeyValue01_partitionBy {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)
    rdd.partitionBy(new MyPartitioner(2)).mapPartitionsWithIndex((index,items)=>items.map((index,_))).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }

}

class MyPartitioner(num:Int) extends Partitioner{
  override def numPartitions: Int =num

  override def getPartition(key: Any): Int = {
    if (key.isInstanceOf[Int]){
      val keyval: Int = key.asInstanceOf[Int]
      if(keyval%2==0){
        0
      }else{
        1
      }
    }else{
      0
    }
  }
}
