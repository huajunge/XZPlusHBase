package com.xzp.experiments.query

import com.xzp.curve.{HBPlusSFC, XZ2SFC, XZPlusSFC, XZSSFC}
import com.xzp.experiments.query.VaryingWindows.{query, squery}
import com.xzp.hbase.HBaseClient

object VaryingResolutions {
  def main(args: Array[String]): Unit = {
    val tablexz = args(0)
    val tablexzs = args(1)
    val tablexzp = args(2)
    val tableHB = args(3)
    val MBR = args(4).split(",")
    var minLon: Double = MBR(0).toDouble
    var minLat: Double = MBR(1).toDouble
    var maxLon: Double = MBR(2).toDouble
    var maxLat: Double = MBR(3).toDouble
    val sPrecision = args(5).toShort
    val ePrecision = args(6).toShort
    //    var minLon: Double = 116.34557
    //    var minLat: Double = 39.92818
    //    var maxLon: Double = 116.35057
    //    var maxLat: Double = 39.93318
    val interval: Double = 0.01
    val offset: Double = 0.01
    val c: HBaseClient = new HBaseClient(tablexz + "_", 16.toShort, XZ2SFC.apply(16));
    c.setPrintLogs(false)
    query(c, minLon, minLat, 0.0001, offset)
    for (i <- sPrecision to ePrecision) {
      val xzPlusSFC: XZPlusSFC = XZPlusSFC.apply(i.toShort)
      val xz2SFC: XZ2SFC = XZ2SFC.apply(i.toShort)
      val hb: HBPlusSFC = HBPlusSFC.apply(i.toShort)
      val xzs: XZSSFC = XZSSFC.apply(i.toShort)
      val xzClient: HBaseClient = new HBaseClient(tablexz + "_" + i, i.toShort, xz2SFC);
      val xzsClient: HBaseClient = new HBaseClient(tablexzs + "_" + i, i.toShort, xzs);
      val xzpClient: HBaseClient = new HBaseClient(tablexzp + "_" + i, i.toShort, xzPlusSFC);
      val xzbClient: HBaseClient = new HBaseClient(tableHB + "_" + i, i.toShort, hb);
      xzClient.setPrintLogs(true)
      xzsClient.setPrintLogs(true)
      xzpClient.setPrintLogs(true)
      xzbClient.setPrintLogs(true)
      println(s"---Precision:$i----")
      println(s"---xz2:$i----")
      query(xzClient, minLon, minLat, interval * 5, offset)
      println(s"---xzs:$i----")
      squery(xzsClient, minLon, minLat, interval * 5, offset)
      println(s"---xzp:$i----")
      query(xzpClient, minLon, minLat, interval * 5, offset)
      println(s"---xzb:$i----")
      query(xzbClient, minLon, minLat, interval * 5, offset)
      println("|||||||||||||||")
      xzClient.close()
      xzsClient.close()
      xzpClient.close()
      xzbClient.close()
    }
    c.close()
  }
}
