package com.xzp.experiments.query

import com.xzp.curve.XZPlusSFC
import com.xzp.experiments.query.VaryingWindows.query
import com.xzp.hbase.HBaseClient

object VaryingWindowsAndResolutions {
  def main(args: Array[String]): Unit = {
    val tablexzp = args(0)
    val MBR = args(1).split(",")
    var minLon: Double = MBR(0).toDouble
    var minLat: Double = MBR(1).toDouble
    var maxLon: Double = MBR(2).toDouble
    var maxLat: Double = MBR(3).toDouble
    val sPrecision = args(2).toShort
    val ePrecision = args(3).toShort
    //    var minLon: Double = 116.34557
    //    var minLat: Double = 39.92818
    //    var maxLon: Double = 116.35057
    //    var maxLat: Double = 39.93318
    val interval: Double = 0.01
    val offset: Double = 0.01
    val c: HBaseClient = new HBaseClient(tablexzp + "_", 16.toShort, XZPlusSFC.apply(16))

    for (i <- sPrecision to ePrecision) {
      val xzPlusSFC: XZPlusSFC = XZPlusSFC.apply(i.toShort);
      val xzpClient: HBaseClient = new HBaseClient(tablexzp + "_" + i, i.toShort, xzPlusSFC);
      println(s"---xzp:$i----")
      query(c, minLon, minLat, 0.0001, offset)
      for (j <- 1 to 10) {
        println(s"---window:$j----")
        query(xzpClient, minLon, minLat, interval * j, offset)
      }
      xzpClient.close()
    }
  }
}
