package com.xzp.curve

class HilbertSFC(g: Short, xBounds: (Double, Double), yBounds: (Double, Double)) extends XZ2SFC(g, xBounds, yBounds) {

  override def sequenceCode(x: Double, y: Double, length: Int): Long = {
    var xmin = 0.0
    var ymin = 0.0
    var xmax = 1.0
    var ymax = 1.0

    var cs = 0L

    var i = 0
    var currentDir = 0
    var of = offset(currentDir, 0)
    while (i < length) {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      (x < xCenter, y < yCenter) match {
        case (true, true) =>
          of = offset(currentDir, 0)
          xmax = xCenter;
          ymax = yCenter
        case (true, false) =>
          of = offset(currentDir, 1)
          xmax = xCenter;
          ymin = yCenter
        case (false, true) =>
          of = offset(currentDir, 2)
          xmin = xCenter;
          ymax = yCenter
        case (false, false) =>
          of = offset(currentDir, 3)
          xmin = xCenter;
          ymin = yCenter
      }
      cs += 1L + of._1 * (math.pow(4, g - i).toLong - 1L) / 3L;
      currentDir = of._2
      i += 1
    }

    cs
  }

  val code = Array(
    Array(0L, 1L, 2L, 3L),
    Array(0L, 3L, 2L, 1L),
    Array(2L, 1L, 0L, 3L),
    Array(2L, 3L, 0L, 1L))
  val dir = Array(
    Array(1, 0, 0, 2),
    Array(0, 3, 1, 1),
    Array(2, 2, 3, 0),
    Array(3, 1, 2, 3))

  def offset(current: Int, quad: Int): (Long, Int) = {
    (code(current)(quad), dir(current)(quad))
  }
}

object HilbertSFC {

  // the initial level of quads
  private val LevelOneElements = XElement(0.0, 0.0, 1.0, 1.0, 1.0).children

  // indicator that we have searched a full level of the quad/oct tree
  private val LevelTerminator = XElement(-1.0, -1.0, -1.0, -1.0, 0)

  private val cache = new java.util.concurrent.ConcurrentHashMap[Short, HilbertSFC]()

  def apply(g: Short): HilbertSFC = {
    var sfc = cache.get(g)
    if (sfc == null) {
      sfc = new HilbertSFC(g, (-180.0, 180.0), (-90.0, 90.0))
      cache.put(g, sfc)
    }
    sfc
  }

  /**
    * Region being queried. Bounds are normalized to [0-1].
    *
    * @param xmin x lower bound in [0-1]
    * @param ymin y lower bound in [0-1]
    * @param xmax x upper bound in [0-1], must be >= xmin
    * @param ymax y upper bound in [0-1], must be >= ymin
    */
  private case class QueryWindow(xmin: Double, ymin: Double, xmax: Double, ymax: Double)

  /**
    * An extended Z curve element. Bounds refer to the non-extended z element for simplicity of calculation.
    *
    * An extended Z element refers to a normal Z curve element that has it's upper bounds expanded by double it's
    * width/height. By convention, an element is always square.
    *
    * @param xmin   x lower bound in [0-1]
    * @param ymin   y lower bound in [0-1]
    * @param xmax   x upper bound in [0-1], must be >= xmin
    * @param ymax   y upper bound in [0-1], must be >= ymin
    * @param length length of the non-extended side (note: by convention width should be equal to height)
    */
  private case class XElement(xmin: Double, ymin: Double, xmax: Double, ymax: Double, length: Double) {

    // extended x and y bounds
    lazy val xext = xmax + length
    lazy val yext = ymax + length

    def isContained(window: QueryWindow): Boolean =
      window.xmin <= xmin && window.ymin <= ymin && window.xmax >= xext && window.ymax >= yext

    def overlaps(window: QueryWindow): Boolean =
      window.xmax >= xmin && window.ymax >= ymin && window.xmin <= xext && window.ymin <= yext

    def children: Seq[XElement] = {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      val len = length / 2.0
      val c0 = copy(xmax = xCenter, ymax = yCenter, length = len)
      val c1 = copy(xmin = xCenter, ymax = yCenter, length = len)
      val c2 = copy(xmax = xCenter, ymin = yCenter, length = len)
      val c3 = copy(xmin = xCenter, ymin = yCenter, length = len)
      Seq(c0, c1, c2, c3)
    }
  }
}
