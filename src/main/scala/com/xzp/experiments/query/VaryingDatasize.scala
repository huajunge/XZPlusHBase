package com.xzp.experiments.query

import com.xzp.AbstractClient
import com.xzp.curve.{XZ2SFC, XZPlusSFC, XZSSFC}
import com.xzp.hbase.HBaseClient

object VaryingDatasize {
  def main(args: Array[String]): Unit = {
    val tableXZ = args(0)
    val tableXZP = args(1)
    val tableXZS = args(2)
    //Storage time
    val lat = 26.21497
    val lon = 106.25618
    val MBR = args(3).split(",")
    var minLon: Double = MBR(0).toDouble
    var minLat: Double = MBR(1).toDouble
    val interval: Double = 0.01
    var offset: Double = 0.01
    println("xz")
    query(new HBaseClient(tableXZ, 16.toShort, XZ2SFC.apply(16)), minLon, minLat, interval * 3, offset)
    println("xzp")
    query(new HBaseClient(tableXZP, 16.toShort, XZPlusSFC.apply(16)), minLon, minLat, interval * 3, offset)
    println("xs")
    querys(new HBaseClient(tableXZS, 16.toShort, XZSSFC.apply(16)), minLon, minLat, interval * 3, offset)
  }

  def query(client: AbstractClient, minLon: Double, minLat: Double, interval: Double, offset: Double): Unit = {
    for (i <- -1 to 1) {
      for (j <- -1 to 1) {
        client.rangeQuery(minLon + offset * i, minLat + offset * j, minLon + offset * i + interval, minLat + offset * j + interval)
      }
    }
  }

  def querys(client: HBaseClient, minLon: Double, minLat: Double, interval: Double, offset: Double): Unit = {
    for (i <- -1 to 1) {
      for (j <- -1 to 1) {
        client.SRangeQuery(minLon + offset * i, minLat + offset * j, minLon + offset * i + interval, minLat + offset * j + interval)
      }
    }
  }
}
