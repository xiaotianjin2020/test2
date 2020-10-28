package com.atguigu.test6

/**
 * @author shkstart 
 * @create 2020-09-28 18:53 
 */
object Test8 {
  def main(args: Array[String]): Unit = {
    val list1: List[Int] = List(1, 2, 3, 4, 5, 6, 7)
    val list2: List[Int] = List(4, 5, 6, 7, 8, 9, 10)
    //（4）集合初始数据（不包含最后一个）
    println(list1.init)
  }

}
