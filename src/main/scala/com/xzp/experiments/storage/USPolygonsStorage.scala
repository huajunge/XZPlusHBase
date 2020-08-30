package com.xzp.experiments.storage

import com.xzp.curve.{HBPlusSFC, XZ2SFC, XZPlusSFC, XZSSFC}
import com.xzp.hbase.HBaseClient

import scala.io.Source

object USPolygonsStorage {
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
    var clients = Array[Array[HBaseClient]]()
    clients = new Array[Array[HBaseClient]](ep - sp + 1)
    for (i <- sp to ep) {
      clients(i - sp) = new Array[HBaseClient](4);
      clients(i - sp)(0) = new HBaseClient(s"${tablexz}_$i", i.toShort, XZ2SFC.apply(i.toShort));
      clients(i - sp)(1) = new HBaseClient(s"${tablexzp}_$i", i.toShort, XZPlusSFC.apply(i.toShort));
      clients(i - sp)(2) = new HBaseClient(s"${tableHB}_$i", i.toShort, HBPlusSFC.apply(i.toShort));
      clients(i - sp)(3) = new HBaseClient(s"${tablexzs}_$i", i.toShort, XZSSFC.apply(i.toShort));
    }
    val rn = Source.fromFile(path)
    //val rn = Source.fromFile("D:\\工作文档\\data\\bj\\Road_Network_BJ_2016Q1_recoding.txt")
    val lines = rn.getLines()
    val roadSegments = lines
    var size = 0
    while (roadSegments.hasNext) {
      val r = roadSegments.next().split(";")
      for (i <- sp to ep) {
        clients(i - sp)(0).batchInsert(r(0), r(1), r(1))
        clients(i - sp)(1).batchInsert(r(0), r(1), r(1))
        clients(i - sp)(2).batchInsert(r(0), r(1), r(1))
        clients(i - sp)(3).sbatchInsert(r(0), r(1), r(1))
      }
      size += 1
      if (size % 10000 == 0) {
        println(size)
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
    rn.close()
  }
}
