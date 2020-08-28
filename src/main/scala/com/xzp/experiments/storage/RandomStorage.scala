package com.xzp.experiments.storage

import java.util.Random

import com.xzp.curve.{HBPlusSFC, XZ2SFC, XZPlusSFC}
import com.xzp.geometry.MinimumBoundingBox
import com.xzp.hbase.HBaseClient

object RandomStorage {
  def main(args: Array[String]): Unit = {
    val tableXZP = "xzp1"
    val tableXZ = "xz1"
    val tableXZB = "xzb1"
    val tableXZS = "xzs1"
    val size = args(0).toInt
    val startIndex = args(1).toInt
    val endIndex = args(2).toInt
    for (i <- startIndex to endIndex) {
      val precision = i.toShort
      val xz2SFC = XZ2SFC.apply(precision)
      val xzPlusSFC = XZPlusSFC.apply(precision)
      val hilbertSFC = HBPlusSFC.apply(precision)
      println(s"----store $i-----")
      var time = System.currentTimeMillis()
      store(tableXZ + "_" + precision, xz2SFC, precision, size)
      println(s"store xz:${System.currentTimeMillis() - time}")
      time = System.currentTimeMillis()
      store(tableXZP + "_" + precision, xzPlusSFC, precision, size)
      println(s"store xzp:${System.currentTimeMillis() - time}")
      time = System.currentTimeMillis()
      store(tableXZB + "_" + precision, hilbertSFC, precision, size)
      println(s"store xzb:${System.currentTimeMillis() - time}")
      time = System.currentTimeMillis()
      storeS(tableXZS + "_" + precision, hilbertSFC, precision, size)
      println(s"store xzs:${System.currentTimeMillis() - time}")
      time = System.currentTimeMillis()
    }
  }

  def store(tableName: String, sfc: XZ2SFC, precision: Short, size: Int): Unit = {
    val lat = 26.21497
    val lon = 106.25618
    try {
      val hBaseClient: HBaseClient = new HBaseClient(tableName, precision, sfc)
      try {
        val random: Random = new Random(1000000)
        val randomLat: Random = new Random(2661497)
        for (k <- 0 until size) {
          for (j <- 1 to 5) {
            val offset: Double = random.nextDouble * 0.5
            val offsetLat: Double = randomLat.nextDouble * 0.5
            //System.out.println(String.format("%s_%s", offset, m));
            val mbr2: MinimumBoundingBox = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005)
            //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
            hBaseClient.batchInsert((k * 5 + j) + "", mbr2.toPolygon(4326).toText, mbr2.toPolygon(4326).toText)
          }
        }
        hBaseClient.finishBatchPut()
      } finally {
        if (hBaseClient != null) hBaseClient.close()
      }
    }
  }

  def storeS(tableName: String, sfc: XZ2SFC, precision: Short, size: Int): Unit = {
    val lat = 26.21497
    val lon = 106.25618
    try {
      val hBaseClient: HBaseClient = new HBaseClient(tableName, precision, sfc)
      try {
        val random: Random = new Random(1000000)
        val randomLat: Random = new Random(2661497)
        for (k <- 0 until size) {
          for (j <- 1 to 5) {
            val offset: Double = random.nextDouble * 0.5
            val offsetLat: Double = randomLat.nextDouble * 0.5
            //System.out.println(String.format("%s_%s", offset, m));
            val mbr2: MinimumBoundingBox = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005)
            //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
            hBaseClient.sbatchInsert((k * 5 + j) + "", mbr2.toPolygon(4326).toText, mbr2.toPolygon(4326).toText)
          }
        }
        hBaseClient.finishBatchPut()
      } finally {
        if (hBaseClient != null) hBaseClient.close()
      }
    }
  }
}
