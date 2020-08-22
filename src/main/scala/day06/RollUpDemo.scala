package day06

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object RollUpDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("data/data.csv")
    //    1	2020/2/18 14:20	2020/2/18 14:46	20

    val beanTupleRDD = rdd1.mapPartitions(it => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      it.map(x => {
        val arr = x.split(",")
        val userId = arr(0)
        val startDate = arr(1)
        val endDate = arr(2)
        val cost = arr(3).toInt
        (LogBean(userId, dateFormat.parse(startDate).getTime, dateFormat.parse(endDate).getTime, cost), null)
      }
      )
    })

    val userIds = beanTupleRDD.map(_._1.userId).distinct().collect()
    implicit val sorter = Ordering[Long].on[LogBean](x => {
      x.startDate
    })
    val sortedWithPartitonRDD = beanTupleRDD.repartitionAndSortWithinPartitions(new UidPartitioner(userIds))
    val resRDD: RDD[((String, Long), (Long, Long))] = sortedWithPartitonRDD.mapPartitions(it => {
      var tmp = 0L
      var flag = 0L
      var sumCost = 0L
      var orinStartDate = 0L
      it.map(logBeanTuple => {
        val logBean = logBeanTuple._1
        val userId = logBean.userId
        val startDate = logBean.startDate
        val endDate = logBean.endDate
        val cost = logBean.cost


        if (tmp != 0) {
          if ((startDate - tmp) / 1000 / 60 > 10) {
            sumCost = 0
            orinStartDate = startDate
          }
          else {
            sumCost += cost
          }
        }
        else (
          orinStartDate = startDate
          )
        tmp = endDate
        ((userId, orinStartDate), (endDate, sumCost))
      }
      )
    })

    resRDD.reduceByKey((t1,t2)=>{
      (Math.max(t1._1,t2._1),Math.max(t1._2,t2._2))
    })
//    resRDD.groupByKey().mapValues(it => {
//      it.toList.sortBy(-_._1).take(1)
//    })
      .saveAsTextFile("data/RollUpDemo")


    sc.stop()
  }

}


case class LogBean(
                    userId: String,
                    startDate: Long,
                    endDate: Long,
                    cost: Int
                  )

class UidPartitioner(uids: Array[String]) extends Partitioner {

  val uidToNum = new mutable.HashMap[String, Int]()
  var i = 0
  for (uid <- uids) {
    uidToNum(uid) = i
    i += 1
  }

  override def numPartitions: Int = uids.length

  override def getPartition(key: Any): Int = {
    val uid = key.asInstanceOf[LogBean].userId
    uidToNum(uid)
  }
}