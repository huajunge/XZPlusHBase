package com.xzp.experiments.query

import com.xzp.AbstractClient
import com.xzp.curve.{HBPlusSFC, XZ2SFC, XZPlusSFC, XZSSFC}
import com.xzp.hbase.HBaseClient

object VaryingWindows {
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
    val precision = args(5).toShort
    //    var minLon: Double = 116.34557
    //    var minLat: Double = 39.92818
    //    var maxLon: Double = 116.35057
    //    var maxLat: Double = 39.93318
    var interval: Double = 0.01
    if (args.length == 7) {
      interval = args(6).toDouble
    }
    var offset: Double = 0.01
    if (args.length == 8) {
      offset = args(7).toDouble
    }
    val xzPlusSFC: XZPlusSFC = XZPlusSFC.apply(precision)
    val xz2SFC: XZ2SFC = XZ2SFC.apply(precision)
    val hb: HBPlusSFC = HBPlusSFC.apply(precision)
    val xzs: XZSSFC = XZSSFC.apply(precision)

    val xzClient: HBaseClient = new HBaseClient(tablexz, precision, xz2SFC);
    val xzsClient: HBaseClient = new HBaseClient(tablexzs, precision, xzs);
    val xzpClient: HBaseClient = new HBaseClient(tablexzp, precision, xzPlusSFC);
    val xzbClient: HBaseClient = new HBaseClient(tableHB, precision, hb);
    val c: HBaseClient = new HBaseClient(tablexz + "_", 16.toShort, XZ2SFC.apply(16));
    c.setPrintLogs(false)
    query(c, minLon, minLat, 0.0001, offset)
    for (i <- 1 to 10) {
      println(s"---xz2:$i----")
      query(xzClient, minLon, minLat, interval * i, offset)
      println(s"---xzs:$i----")
      squery(xzsClient, minLon, minLat, interval * i, offset)
      println(s"---xzp:$i----")
      query(xzpClient, minLon, minLat, interval * i, offset)
      println(s"---xzb:$i----")
      query(xzbClient, minLon, minLat, interval * i, offset)
    }
    xzClient.close()
    xzsClient.close()
    xzpClient.close()
    xzbClient.close()
    c.close()
  }

  def query(client: AbstractClient, minLon: Double, minLat: Double, interval: Double, offset: Double): Unit = {
    for (i <- -3 to 3) {
      for (j <- -3 to 3) {
        client.rangeQuery(minLon + offset * i, minLat + offset * j, minLon + offset * i + interval, minLat + offset * j + interval)
      }
    }
  }

  def squery(client: HBaseClient, minLon: Double, minLat: Double, interval: Double, offset: Double): Unit = {
    for (i <- -3 to 3) {
      for (j <- -3 to 3) {
        client.SRangeQuery(minLon + offset * i, minLat + offset * j, minLon + offset * i + interval, minLat + offset * j + interval)
      }
    }
  }
}
