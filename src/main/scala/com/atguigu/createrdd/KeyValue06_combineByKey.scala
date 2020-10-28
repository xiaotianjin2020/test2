package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 19:27 
 */
object KeyValue06_combineByKey {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val input: RDD[(String, Int)] = sc.makeRDD(list, 2)
    //第一步 将 (v)=>(v,1)
    val combineRdd: RDD[(String, (Int, Int))] = input.combineByKey((_,1),(acc:(Int,Int),v)=>((acc._1+v),acc._2+1),(acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc2._2+acc2._2))
    combineRdd.collect().foreach(println)

    combineRdd.map{
      case(key,value)=>{
      (key,value._1/value._2.toDouble)
    }
    }.collect().foreach(println)

    //第二步将(acc,1),v=>(acc._1+v,acc._2+1)

    //第三步 (acc._1+v,acc._2+1)=>(_+_,_+_)



    //4.关闭连接
    sc.stop()
  }

}
