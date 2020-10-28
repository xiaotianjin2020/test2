package com.atguigu.test4

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author shkstart 
 * @create 2020-09-27 16:44 
 */
object accumulator03_define {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    //3. 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Spark", "Spark"), 2)
    //创建累加器
    val acc:MyAccumulator = new MyAccumulator
    //注册累加器
    sc.register(acc,"wordcount")
    //使用累加器
    rdd.foreach(word=>{
      acc.add(word)
    })
    //获取累加器的累加结果
    println(acc.value)

    //4.关闭连接
    sc.stop()
  }

}

class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
  //定义输出数据集合
  var map = mutable.Map[String, Long]()

  //是否为初始化状态 ，即为初始化状态
  override def isZero: Boolean = map.isEmpty

  //赋值累加器
  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] ={
    new MyAccumulator
  }
//重置累加器
  override def reset(): Unit = map.clear()
//增加数据
  override def add(v: String): Unit = {
    //业务逻辑
    if(v.startsWith("H")){
      map(v)=map.getOrElse(v,0L)+1L
    }
  }
//合并累加器
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    other.value.foreach{
      case(word,count)=>{
        map(word)=map.getOrElse(word,0L)+count
      }
    }
  }
//累加器的值其实就是累加器返回的值
  override def value: mutable.Map[String, Long] = map
}
