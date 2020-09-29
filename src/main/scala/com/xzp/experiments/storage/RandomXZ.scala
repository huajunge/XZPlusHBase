package com.xzp.experiments.storage

import java.io.IOException
import java.util.Random

import com.xzp.curve.{XZ2SFC, XZSSFC}
import com.xzp.geometry.MinimumBoundingBox
import com.xzp.hbase.HBaseClient

object RandomXZ {
  def main(args: Array[String]): Unit = {
    val size = args(0).toLong
    val tableXZ = args(1)
    val tableXZP = args(2)
    val tableXZS = args(3)
    //Storage time
    val lat = 26.21497
    val lon = 106.25618
    val MBR = args(4).split(",")
    var minLon: Double = MBR(0).toDouble
    var minLat: Double = MBR(1).toDouble
    val interval: Double = 0.01
    var offset: Double = 0.01

    println("|||||||||||||||||||||")
    //    val namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("demo-pool-%d").build
    //    val singleThreadPool = new ThreadPoolExecutor(10, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable](1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy)
    //    for (i <- 1 to 5) {
    //      singleThreadPool.execute(() -> {
    //        try {
    //          val hBaseClient = new HBaseClient(tableXZP + "_" + i)
    //          try {
    //            val random = new Random(1000000)
    //            val randomLat = new Random(2661497)
    //            //println(s"Data size:$i")
    //            var time = System.currentTimeMillis()
    //            for (k <- 1 to size.toInt * Math.pow(10, i - 1).toInt) {
    //              for (j <- 1 to 5) {
    //                val offset = random.nextDouble * 0.5
    //                val offsetLat = randomLat.nextDouble * 0.5
    //                //System.out.println(String.format("%s_%s", offset, m));
    //                val mbr2 = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005)
    //                //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
    //                hBaseClient.batchInsert((k * 5 + j) + "", mbr2.toPolygon(4326).toText, mbr2.toPolygon(4326).toText)
    //              }
    //            }
    //            hBaseClient.finishBatchPut()
    //            //println(s"storage time: ${System.currentTimeMillis() - time}")
    //          } catch {
    //            case e@(_: InterruptedException | _: IOException) =>
    //              e.printStackTrace()
    //          } finally if (hBaseClient != null) hBaseClient.close()
    //        }
    //      })
    //    }
    //
    //    for (i <- 1 to 5) {
    //      singleThreadPool.execute(() -> {
    //        try {
    //          val hBaseClient = new HBaseClient(tableXZ + "_" + i, 16.toShort, XZ2SFC.apply(16))
    //          try {
    //            val random = new Random(1000000)
    //            val randomLat = new Random(2661497)
    //            println(s"Data size:$i")
    //            var time = System.currentTimeMillis()
    //            for (k <- 1 to size.toInt * Math.pow(10, i - 1).toInt) {
    //              for (j <- 1 to 5) {
    //                val offset = random.nextDouble * 0.5
    //                val offsetLat = randomLat.nextDouble * 0.5
    //                //System.out.println(String.format("%s_%s", offset, m));
    //                val mbr2 = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005)
    //                //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
    //                hBaseClient.batchInsert((k * 5 + j) + "", mbr2.toPolygon(4326).toText, mbr2.toPolygon(4326).toText)
    //              }
    //            }
    //            hBaseClient.finishBatchPut()
    //          } catch {
    //            case e@(_: InterruptedException | _: IOException) =>
    //              e.printStackTrace()
    //          } finally if (hBaseClient != null) hBaseClient.close()
    //        }
    //      })
    //    }
    //
    //    for (i <- 1 to 5) {
    //      singleThreadPool.execute(() -> {
    //        try {
    //          val hBaseClient = new HBaseClient(tableXZS + "_" + i, 16.toShort, XZSSFC.apply(16))
    //          try {
    //            val random = new Random(1000000)
    //            val randomLat = new Random(2661497)
    //            println(s"Data size:$i")
    //            var time = System.currentTimeMillis()
    //            for (k <- 1 to size.toInt * Math.pow(10, i - 1).toInt) {
    //              for (j <- 1 to 5) {
    //                val offset = random.nextDouble * 0.5
    //                val offsetLat = randomLat.nextDouble * 0.5
    //                //System.out.println(String.format("%s_%s", offset, m));
    //                val mbr2 = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005)
    //                //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
    //                hBaseClient.sbatchInsert((k * 5 + j) + "", mbr2.toPolygon(4326).toText, mbr2.toPolygon(4326).toText)
    //              }
    //            }
    //            hBaseClient.finishBatchPut()
    //          } catch {
    //            case e@(_: InterruptedException | _: IOException) =>
    //              e.printStackTrace()
    //          } finally if (hBaseClient != null) hBaseClient.close()
    //        }
    //      })
    //    }
    //    singleThreadPool.shutdown()

    for (i <- 1 to 5) {
      try {
        val hBaseClient = new HBaseClient(tableXZP + "_" + i)
        try {
          val random = new Random(1000000)
          val randomLat = new Random(2661497)
          println(s"Data size:$i")
          var time = System.currentTimeMillis()
          for (k <- 1 to size.toInt * Math.pow(10, i - 1).toInt) {
            for (j <- 1 to 5) {
              val offset = random.nextDouble * 0.5
              val offsetLat = randomLat.nextDouble * 0.5
              //System.out.println(String.format("%s_%s", offset, m));
              val mbr2 = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005)
              //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
              hBaseClient.batchInsert((k * 5 + j) + "", mbr2.toPolygon(4326).toText, mbr2.toPolygon(4326).toText)
            }
          }
          hBaseClient.finishBatchPut()
        } catch {
          case e@(_: InterruptedException | _: IOException) =>
            e.printStackTrace()
        } finally if (hBaseClient != null) hBaseClient.close()
      }
    }

    for (i <- 1 to 5) {
      try {
        val hBaseClient = new HBaseClient(tableXZ + "_" + i, 16.toShort, XZ2SFC.apply(16))
        try {
          val random = new Random(1000000)
          val randomLat = new Random(2661497)
          println(s"Data size:$i")
          var time = System.currentTimeMillis()
          for (k <- 1 to size.toInt * Math.pow(10, i - 1).toInt) {
            for (j <- 1 to 5) {
              val offset = random.nextDouble * 0.5
              val offsetLat = randomLat.nextDouble * 0.5
              //System.out.println(String.format("%s_%s", offset, m));
              val mbr2 = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005)
              //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
              hBaseClient.batchInsert((k * 5 + j) + "", mbr2.toPolygon(4326).toText, mbr2.toPolygon(4326).toText)
            }
          }
          hBaseClient.finishBatchPut()
        } catch {
          case e@(_: InterruptedException | _: IOException) =>
            e.printStackTrace()
        } finally if (hBaseClient != null) hBaseClient.close()
      }
    }


    for (i <- 1 to 5) {
      try {
        val hBaseClient = new HBaseClient(tableXZS + "_" + i, 16.toShort, XZSSFC.apply(16))
        try {
          val random = new Random(1000000)
          val randomLat = new Random(2661497)
          println(s"Data size:$i")
          var time = System.currentTimeMillis()
          for (k <- 1 to size.toInt * Math.pow(10, i - 1).toInt) {
            for (j <- 1 to 5) {
              val offset = random.nextDouble * 0.5
              val offsetLat = randomLat.nextDouble * 0.5
              //System.out.println(String.format("%s_%s", offset, m));
              val mbr2 = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005)
              //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
              hBaseClient.sbatchInsert((k * 5 + j) + "", mbr2.toPolygon(4326).toText, mbr2.toPolygon(4326).toText)
            }
          }
          hBaseClient.finishBatchPut()
        } catch {
          case e@(_: InterruptedException | _: IOException) =>
            e.printStackTrace()
        } finally if (hBaseClient != null) hBaseClient.close()
      }
    }
  }
}
