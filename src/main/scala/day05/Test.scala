package day05

import scala.io.Source

/**
  * @author wangyn3
  * @date 2020-08-12 21:30
  */
object Test {

  def main(args: Array[String]): Unit = {
    val data:Iterator[String]=Source.fromFile("data/aa.txt").getLines()
    data.filter(x=>{
      x.contains("转诊")&&(!x.contains("未转诊") )
    }).foreach(x=>{
      println(x.split(":")(0))
    })
  }
}
