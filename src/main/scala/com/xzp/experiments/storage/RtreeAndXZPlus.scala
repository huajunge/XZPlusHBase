package com.xzp.experiments.storage

import java.io.IOException
import java.util.Random

import com.xzp.AbstractClient
import com.xzp.geometry.MinimumBoundingBox
import com.xzp.hbase.{HBaseClient, RtreeHBaseClient}
import org.apache.lucene.util.RamUsageEstimator

object RtreeAndXZPlus {
  def main(args: Array[String]): Unit = {
    val size = args(0).toLong
    val tableR = args(1)
    val tableXZP = args(2)
    //Storage time
    val lat = 26.21497
    val lon = 106.25618
    val MBR = args(3).split(",")
    var minLon: Double = MBR(0).toDouble
    var minLat: Double = MBR(1).toDouble
    val interval: Double = 0.01
    var offset: Double = 0.01
    for (i <- 1 to 5) {
      try {
        val hBaseClient: RtreeHBaseClient = new RtreeHBaseClient(tableR + "_" + i)
        try {
          val random: Random = new Random(1000000)
          val randomLat: Random = new Random(2661497)
          println(s"Data size:$i")
          val time = System.currentTimeMillis()
          for (k <- 1 to size.toInt * Math.pow(10, i - 1).toInt) {
            for (j <- 1 to 5) {
              val offset: Double = random.nextDouble * 0.5
              val offsetLat: Double = randomLat.nextDouble * 0.5
              //System.out.println(String.format("%s_%s", offset, m));
              val mbr2: MinimumBoundingBox = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005)
              //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
              hBaseClient.batchInsert((k * 5 + j) + "", mbr2.toPolygon(4326).toText, mbr2.toPolygon(4326).toText)
            }
          }
          println("convert time" + (System.currentTimeMillis() - time))
          hBaseClient.finishBatchPut()
          println("storage time" + (System.currentTimeMillis() - time))
          System.out.println(RamUsageEstimator.humanSizeOf(hBaseClient.getrTree()))
          for (k <- 3 to 3) {
            println(s"------------$k----")
            query(hBaseClient, minLon, minLat, interval * k, offset)
          }
        } catch {
          case e@(_: InterruptedException | _: IOException) =>
            e.printStackTrace()
        } finally if (hBaseClient != null) hBaseClient.close()
      }
    }
    println("|||||||||||||||||||||")
    for (i <- 1 to 5) {
      try {
        val hBaseClient: HBaseClient = new HBaseClient(tableXZP + "_" + i)
        try {
          val random: Random = new Random(1000000)
          val randomLat: Random = new Random(2661497)
          println(s"Data size:$i")
          val time = System.currentTimeMillis()
          for (k <- 1 to size.toInt * Math.pow(10, i - 1).toInt) {
            for (j <- 1 to 5) {
              val offset: Double = random.nextDouble * 0.5
              val offsetLat: Double = randomLat.nextDouble * 0.5
              //System.out.println(String.format("%s_%s", offset, m));
              val mbr2: MinimumBoundingBox = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005)
              //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
              hBaseClient.batchInsert((k * 5 + j) + "", mbr2.toPolygon(4326).toText, mbr2.toPolygon(4326).toText)
            }
          }
          println("convert time" + (System.currentTimeMillis() - time))
          hBaseClient.finishBatchPut()
          println("storage time" + (System.currentTimeMillis() - time))
          for (k <- 3 to 3) {
            println(s"------------$k----")
            query(hBaseClient, minLon, minLat, interval * k, offset)
          }
        } catch {
          case e@(_: InterruptedException | _: IOException) =>
            e.printStackTrace()
        } finally if (hBaseClient != null) hBaseClient.close()
      }
    }
  }

    def query(client: AbstractClient, minLon: Double, minLat: Double, interval: Double, offset: Double): Unit = {
      for (i <- -1 to 1) {
        for (j <- -1 to 1) {
          client.rangeQuery(minLon + offset * i, minLat + offset * j, minLon + offset * i + interval, minLat + offset * j + interval)
        }
      }
    }
}
