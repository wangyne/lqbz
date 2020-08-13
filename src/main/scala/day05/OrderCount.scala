package day05

import com.alibaba.fastjson.{JSON, JSONException}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wangyn3
  * @date 2020-08-13 11:18
  */
object OrderCount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("OrderCount").setMaster("local[*]")
    val sc=new SparkContext(conf)

    val lines:RDD[String]=sc.textFile("data/orders.txt")
    var bean:CaseOrderBean=null
    val beanRDD:RDD[CaseOrderBean]=lines.map((x:String)=>{
      try {
         bean = JSON.parseObject(x, classOf[CaseOrderBean])
      } catch {
        case e:JSONException=>{
          println(e.toString+"...."+x)
        }
      }
      bean
    })
    val cidRDD=beanRDD.map((x:CaseOrderBean)=>{
      (x.cid,x.money)
    })
    val reduceRDD=cidRDD.reduceByKey(_+_)
    val dimCRDD:RDD[Tuple2[String,String]]=sc.textFile("data/dim_c.txt").map(x=>{
      val s=x.split("，")
      (s(0),s(1))
    })

    val joinedRDD=reduceRDD.leftOuterJoin(dimCRDD)
    val resultRDD=joinedRDD.foreach(x=>{
      println(x._1,x._2._2,x._2._1)
    })

  }
}


//{"oid":"o125", "cid": 3, "money": 100.0, "longitude":118.397128,"latitude":35.916527}
 case class CaseOrderBean(val oid:String,
    val cid:String,
    val money:Double,
    val longitude:Double,
    val latitude:Double){


}