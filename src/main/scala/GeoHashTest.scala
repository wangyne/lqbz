import java.util.Properties

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.SparkSession

/**
  * @author wangyn3
  * @date 2020-08-10 17:16
  */
object GeoHashTest {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[2]").appName("GeoHashTest").getOrCreate()
    val url="jdbc:mysql://localhost:3306/test?characterEncoding=utf8&rewriteBatchedStatements=true&useSSL=false"
    val prop=new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123456")

    import spark.implicits._

    val hospJdbc=spark.read.jdbc(url,"hospital",prop).filter("longitude<>'-99' and abs(longitude)<180 and abs(latitude)<180")
//    hospJdbc.show(100)
    hospJdbc.map(row=>{
      val hosp_id=row.getAs[String]("hosp_id")
      val hosp_name=row.getAs[String]("hosp_name")
      val province_name=row.getAs[String]("province_name")
      val city_name=row.getAs[String]("city_name")
      val county_name=row.getAs[String]("county_name")
      val longitude=row.getAs[Double]("longitude")
      val latitude=row.getAs[Double]("latitude")
      val geoHash = GeoHash.withCharacterPrecision(latitude,longitude,5)
      val binaryCode = geoHash.toBinaryString()
      (hosp_id,hosp_name,province_name,province_name,city_name,county_name,longitude,latitude,binaryCode)
    }).show(10,false)

    spark.close()
  }
}
