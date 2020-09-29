package com.xzp.experiments.storage

import java.util.Random

import com.github.davidmoten.rtree.RTree
import com.github.davidmoten.rtree.geometry.{Geometries, Rectangle}
import com.xzp.AbstractClient
import com.xzp.curve.{HBPlusSFC, XZ2SFC, XZPlusSFC, XZSSFC}
import com.xzp.geometry.MinimumBoundingBox
import com.xzp.utils.WKTUtils

object ConvertTime {
  def main(args: Array[String]): Unit = {
    val size = args(0).toLong
    //Storage time
    val lat = 26.21497
    val lon = 106.25618

    val xz = XZ2SFC.apply(16)
    val xzp = XZPlusSFC.apply(16)
    val hbp = HBPlusSFC.apply(16)
    val xzs = XZSSFC.apply(16)
    println("xz|||||||||||||||||||||")
    conver(xz, lon, lat, size)
    println("xzp|||||||||||||||||||||")
    conver(xzp, lon, lat, size)
    println("hbp|||||||||||||||||||||")
    conver(hbp, lon, lat, size)
    println("xzs|||||||||||||||||||||")
    conver(xzs, lon, lat, size)
    println("rtree|||||||||||||||||||||")
    converR(lon, lat, size)
  }

  def query(client: AbstractClient, minLon: Double, minLat: Double, interval: Double, offset: Double): Unit = {
    for (i <- -1 to 1) {
      for (j <- -1 to 1) {
        client.rangeQuery(minLon + offset * i, minLat + offset * j, minLon + offset * i + interval, minLat + offset * j + interval)
      }
    }
  }

  def conver(sfc: XZ2SFC, lon: Double, lat: Double, size: Long): Unit = {
    for (i <- 1 to 5) {
      try {
        try {
          val random: Random = new Random(1000000)
          val randomLat: Random = new Random(2661497)
          println(s"Data size:$i")
          var time = System.currentTimeMillis()
          for (k <- 1 to size.toInt * Math.pow(10, i - 1).toInt) {
            for (j <- 1 to 5) {
              val offset: Double = random.nextDouble * 0.5
              val offsetLat: Double = randomLat.nextDouble * 0.5
              //System.out.println(String.format("%s_%s", offset, m));
              val mbr2: MinimumBoundingBox = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005)
              //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
              val index: Long = sfc.index(mbr2.getMinX, mbr2.getMinY, mbr2.getMaxX, mbr2.getMaxY, false)
            }
          }
          println("storage time " + (System.currentTimeMillis() - time))
        }
      }
    }
  }

  def conver(sfc: XZSSFC, lon: Double, lat: Double, size: Long): Unit = {
    for (i <- 1 to 5) {
      try {
        try {
          val random: Random = new Random(1000000)
          val randomLat: Random = new Random(2661497)
          println(s"Data size:$i")
          var time = System.currentTimeMillis()
          for (k <- 1 to size.toInt * Math.pow(10, i - 1).toInt) {
            for (j <- 1 to 5) {
              val offset: Double = random.nextDouble * 0.5
              val offsetLat: Double = randomLat.nextDouble * 0.5
              //System.out.println(String.format("%s_%s", offset, m));
              val mbr2: MinimumBoundingBox = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005)
              //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
              val index = sfc.indexPositionCode(mbr2.getMinX, mbr2.getMinY, mbr2.getMaxX, mbr2.getMaxY, false)
            }
          }
          println("storage time " + (System.currentTimeMillis() - time))
        }
      }
    }
  }

  def converR(lon: Double, lat: Double, size: Long): Unit = {
    for (i <- 1 to 5) {
      try {
        try {
          var rTree = RTree.create[String, Rectangle]
          val random: Random = new Random(1000000)
          val randomLat: Random = new Random(2661497)
          println(s"Data size:$i")
          var time = System.currentTimeMillis()
          for (k <- 1 to size.toInt * Math.pow(10, i - 1).toInt) {
            for (j <- 1 to 5) {
              val offset: Double = random.nextDouble * 0.5
              val offsetLat: Double = randomLat.nextDouble * 0.5
              //System.out.println(String.format("%s_%s", offset, m));
              val mbr2: MinimumBoundingBox = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005)
              //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
              val geom = mbr2.toPolygon(4326).toText
              val geometry = WKTUtils.read(geom)
              assert(geometry != null)
              val bbox = geometry.getEnvelopeInternal
              rTree = rTree.add((k * 5 + j) + "", Geometries.rectangleGeographic(bbox.getMinX, bbox.getMinY, bbox.getMaxX, bbox.getMaxY))
            }
          }
          println("storage time " + (System.currentTimeMillis() - time))
        }
      }
    }
  }
}
