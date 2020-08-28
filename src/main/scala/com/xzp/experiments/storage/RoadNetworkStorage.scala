package com.xzp.experiments.storage

import java.util

import com.xzp.curve.{HBPlusSFC, XZ2SFC, XZPlusSFC, XZSSFC}
import com.xzp.hbase.HBaseClient
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.VoidFunction
import org.locationtech.jts.geom.impl.CoordinateArraySequence
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, LineString, PrecisionModel}

object RoadNetworkStorage {
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val tablexz = args(1)
    val tablexzp = args(2)
    val tableHB = args(3)
    val tablexzs = args(4)
    //String tableHB = args[3];
    val sp = args(5).toShort
    val ep = args(6).toShort
    val isLocal = args(7).toBoolean
    for (i <- sp to ep) {
      val clientXZ = new HBaseClient(s"${tablexz}_$i", i.toShort, XZ2SFC.apply(i.toShort));
      val clientXZP = new HBaseClient(s"${tablexzp}_$i", i.toShort, XZPlusSFC.apply(i.toShort));
      val clientXZB = new HBaseClient(s"${tableHB}_$i", i.toShort, HBPlusSFC.apply(i.toShort));
      val clientXZS = new HBaseClient(s"${tablexzs}_$i", i.toShort, XZSSFC.apply(i.toShort));
      clientXZ.close()
      clientXZP.close()
      clientXZB.close()
      clientXZS.close()
    }
//    val rn = Source.fromFile("D:\\工作文档\\data\\bj\\Road_Network_BJ_2016Q1_recoding.txt")
//    val lines = rn.getLines()
//    val roadSegments = lines.sliding(10000, 10000)
//    while (roadSegments.hasNext) {
//
//      roadSegments.next().foreach(v => {
//
//      })
//    }
//    rn.close()
    val sparkConf = new SparkConf().setAppName("test")
    if (isLocal) sparkConf.setMaster("local[*]")
    val sparkContext = new JavaSparkContext(sparkConf)
    val tRDD = sparkContext.textFile("D:\\工作文档\\data\\bj\\Road_Network_BJ_2016Q1_recoding.txt", 100)
    tRDD.foreachPartition(new VoidFunction[util.Iterator[String]] {
      override def call(iterable: util.Iterator[String]): Unit = {
        var clients = Array[Array[HBaseClient]]()
        clients = new Array[Array[HBaseClient]](ep - sp + 1)
        for (i <- sp to ep) {
          clients(i - sp) = new Array[HBaseClient](4);
          clients(i - sp)(0) = new HBaseClient(s"${tablexz}_$i", i.toShort, XZ2SFC.apply(i.toShort));
          clients(i - sp)(1) = new HBaseClient(s"${tablexzp}_$i", i.toShort, XZPlusSFC.apply(i.toShort));
          clients(i - sp)(2) = new HBaseClient(s"${tableHB}_$i", i.toShort, HBPlusSFC.apply(i.toShort));
          clients(i - sp)(3) = new HBaseClient(s"${tablexzs}_$i", i.toShort, XZSSFC.apply(i.toShort));
        }
        while (iterable.hasNext) {
          val r = roadSegmentParse(iterable.next, ",", ";")
          for (i <- sp to ep) {
            clients(i - sp)(0).batchInsert(r._1, r._2.toText, r._2.toText)
            clients(i - sp)(1).batchInsert(r._1, r._2.toText, r._2.toText)
            clients(i - sp)(2).batchInsert(r._1, r._2.toText, r._2.toText)
            clients(i - sp)(3).sbatchInsert(r._1, r._2.toText, r._2.toText)
          }
        }
        for (i <- sp to ep) {
          clients(i - sp)(0).finishBatchPut()
          clients(i - sp)(1).finishBatchPut()
          clients(i - sp)(2).finishBatchPut()
          clients(i - sp)(3).finishBatchPut()
          clients(i - sp)(0).close()
          clients(i - sp)(1).close()
          clients(i - sp)(2).close()
          clients(i - sp)(3).close()
        }
      }
    })
    //    tRDD.foreachPartition(iterable => {
    //      var clients = Array[Array[HBaseClient]]()
    //      clients = new Array[Array[HBaseClient]](ep - sp + 1)
    //      for (i <- sp to ep) {
    //        clients(i - sp) = new Array[HBaseClient](4);
    //        clients(i - sp)(0) = new HBaseClient(s"${tablexz}_$i", i.toShort, XZ2SFC.apply(i.toShort));
    //        clients(i - sp)(1) = new HBaseClient(s"${tablexzp}_$i", i.toShort, XZPlusSFC.apply(i.toShort));
    //        clients(i - sp)(2) = new HBaseClient(s"${tableHB}_$i", i.toShort, HBPlusSFC.apply(i.toShort));
    //        clients(i - sp)(3) = new HBaseClient(s"${tablexzs}_$i", i.toShort, XZSSFC.apply(i.toShort));
    //      }
    //      while (iterable.hasNext) {
    //        val r = roadSegmentParse(iterable.next, ",", ";")
    //        for (i <- sp to ep) {
    //          clients(i - sp)(0).batchInsert(r._1, r._2.toText, r._2.toText)
    //          clients(i - sp)(1).batchInsert(r._1, r._2.toText, r._2.toText)
    //          clients(i - sp)(2).batchInsert(r._1, r._2.toText, r._2.toText)
    //          clients(i - sp)(3).sbatchInsert(r._1, r._2.toText, r._2.toText)
    //        }
    //      }
    //      for (i <- sp to ep) {
    //        clients(i - sp)(0).finishBatchPut()
    //        clients(i - sp)(1).finishBatchPut()
    //        clients(i - sp)(2).finishBatchPut()
    //        clients(i - sp)(3).finishBatchPut()
    //        clients(i - sp)(0).close()
    //        clients(i - sp)(1).close()
    //        clients(i - sp)(2).close()
    //        clients(i - sp)(3).close()
    //      }
    //    })
  }

  def roadSegmentParse(next: String, separator1: String, separator2: String): (String, LineString) = {
    val record = next.split(separator1)
    val coordsIndex = record.length - 1
    val coords = record(coordsIndex).split(separator2)
    val edgeID = record(0)
    //    val lineString = new LineString()
    val point = new Array[Coordinate](coords.size)
    val points = coords.map(p => {
      val latLngArr = p.split(" ")
      val lat = latLngArr(0).toDouble
      val lng = latLngArr(1).toDouble
      new Coordinate(lng, lat)
    })
    val c = new CoordinateArraySequence(points)
    val lineString = new LineString(c, new GeometryFactory(new PrecisionModel, 4326))
    (edgeID, lineString)
  }
}
