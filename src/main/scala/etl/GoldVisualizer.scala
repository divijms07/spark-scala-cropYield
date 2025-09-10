import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths}
import java.util.Arrays
import scala.collection.JavaConverters._

// XChart (plotting) imports
import org.knowm.xchart._
import org.knowm.xchart.style.Styler
import org.knowm.xchart.style.markers.SeriesMarkers
import org.knowm.xchart.XYSeries
import java.awt.{BasicStroke, Color}

object GoldVisualizer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Visualize-Gold")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // -------------------------
    // 1) Load first CSV from Gold folder (same as Python)
    // -------------------------
    val goldCsvDir = "src/main/resources/data/gold/crop_yield_v8_csv"
    ensureDir("src/plots")

    val firstCsvPath = firstCsv(goldCsvDir)
      .getOrElse(throw new java.io.FileNotFoundException(s"No CSV file found in gold folder: $goldCsvDir"))

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(firstCsvPath)
      .cache()

    println("✅ Gold Data Loaded for Visualization")
    df.printSchema()
    df.show(5, truncate = false)

    // Columns used in plots (same as Python)
    val yieldCol   = "Yield_tons_per_hectare"
    val supportCol = "Support_Level"
    val rainDevCol = "Rainfall_Deviation"
    val stressCol  = "Climate_Stress_Index"

// -------------------------
// Plot 1: Impact of Agricultural Support (bar: mean yield)
// -------------------------
if (hasCols(df, supportCol, yieldCol)) {
  val grp = df.groupBy(col(supportCol))
    .agg(avg(col(yieldCol)).alias("avg_yield"))
    .orderBy(col(supportCol).asc)

  println("📊 Grouped Data for Bar Chart:")
  grp.show()

  val cats: Array[String] =
    grp.select(col(supportCol).cast("string")).collect().map(_.getString(0))

  val vals: Array[Double] =
    grp.select(col("avg_yield").cast("double")).na.drop().collect().map(_.getDouble(0))

  if (cats.isEmpty || vals.isEmpty) {
    println("⚠️ No data available for bar chart. Skipping plot.")
  } else {
    val chart = new CategoryChartBuilder()
      .width(700).height(500)
      .theme(Styler.ChartTheme.GGPlot2)
      .title("Impact of Fertilizer & Irrigation on Yield")
      .xAxisTitle("Support Level (0=None, 1=Either, 2=Both)")
      .yAxisTitle("Yield (tons per hectare)")
      .build()


      chart.getStyler.setDefaultSeriesRenderStyle(CategorySeries.CategorySeriesRenderStyle.Bar)
      chart.getStyler.setLabelsVisible(true)


    val catList = Arrays.asList(cats: _*)
    val valList = vals.map(java.lang.Double.valueOf).toList.asJava

    chart.addSeries("Mean Yield", catList, valList)

    BitmapEncoder.saveBitmap(chart, "src/plots/yield_by_support.png", BitmapEncoder.BitmapFormat.PNG)
    println("✅ Saved: src/plots/yield_by_support.png")
  }
} else {
  println(s"⚠️ Skipping 'yield_by_support' — missing: '$supportCol' or '$yieldCol'")
}

    // -------------------------
    // Plot 2: Yield vs Rainfall Deviation (scatter + vertical line at x=0)
    // -------------------------
    if (hasCols(df, rainDevCol, yieldCol)) {
      val rows = df.select(col(rainDevCol).cast("double"), col(yieldCol).cast("double"))
        .na.drop()
        .limit(50000) // driver safety
        .collect()

      val xs = rows.map(_.getDouble(0))
      val ys = rows.map(_.getDouble(1))
      val yMin = if (ys.nonEmpty) ys.min else 0.0
      val yMax = if (ys.nonEmpty) ys.max else 1.0

      val chart = new XYChartBuilder()
        .width(700).height(500)
        .theme(Styler.ChartTheme.GGPlot2)
        .title("Yield vs Rainfall Deviation")
        .xAxisTitle("Rainfall Deviation (cm)")
        .yAxisTitle("Yield (tons per hectare)")
        .build()

      val series = chart.addSeries("Data", xs, ys)
//      series.setMarker(SeriesMarkers.CIRCLE)
//      series.setMarkerSize(4)

      // Vertical reference line at x = 0
      val ref = chart.addSeries("Avg Rainfall", Array(0.0, 0.0), Array(yMin, yMax))
      ref.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Line)
      ref.setMarker(SeriesMarkers.NONE)
      ref.setLineStyle(new BasicStroke(1.5f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10.0f, Array(5.0f), 0.0f))
      ref.setLineColor(new Color(220, 20, 60)) // crimson

      BitmapEncoder.saveBitmap(chart, "src/plots/yield_by_support", BitmapEncoder.BitmapFormat.PNG)
      BitmapEncoder.saveBitmap(chart, "src/plots/yield_vs_rainfall", BitmapEncoder.BitmapFormat.PNG)
      BitmapEncoder.saveBitmap(chart, "src/plots/yield_vs_climate_stress", BitmapEncoder.BitmapFormat.PNG)

//      BitmapEncoder.saveBitmap(chart, "src/plots", BitmapEncoder.BitmapFormat.PNG)
      println("✅ Saved: ../plots/yield_vs_rainfall.png")
    } else {
      println(s"⚠️ Skipping 'yield_vs_rainfall' — missing: '$rainDevCol' or '$yieldCol'")
    }

    // -------------------------
    // Plot 3: Climate Stress Impact (scatter)
    // -------------------------
    if (hasCols(df, stressCol, yieldCol)) {
      val rows = df.select(col(stressCol).cast("double"), col(yieldCol).cast("double"))
        .na.drop()
        .limit(50000)
        .collect()

      val xs = rows.map(_.getDouble(0))
      val ys = rows.map(_.getDouble(1))

      val chart = new XYChartBuilder()
        .width(700).height(500)
        .theme(Styler.ChartTheme.GGPlot2)
        .title("Impact of Climate Stress on Yield")
        .xAxisTitle("Climate Stress Index")
        .yAxisTitle("Yield (tons per hectare)")
        .build()

      val series = chart.addSeries("Data", xs, ys)
      series.setMarker(SeriesMarkers.CIRCLE)
//      series.setMarkerSize(4)

      BitmapEncoder.saveBitmap(chart, "src/plots/yield_vs_climate_stress.png", BitmapEncoder.BitmapFormat.PNG)
      println("✅ Saved: ../plots/yield_vs_climate_stress.png")
    } else {
      println(s"⚠️ Skipping 'yield_vs_climate_stress' — missing: '$stressCol' or '$yieldCol'")
    }

    println("✅ Visualizations saved in ../plots/")
    spark.stop()
  }

  // ---------- helpers ----------

  private def ensureDir(dir: String): Unit = {
    try Files.createDirectories(Paths.get(dir))
    catch { case _: Throwable => () }
  }

  private def hasCols(df: DataFrame, cols: String*): Boolean =
    cols.forall(df.columns.contains)

  private def firstCsv(dir: String): Option[String] = {
    val p = Paths.get(dir)
    if (!Files.exists(p) || !Files.isDirectory(p)) return None
    val stream = Files.list(p)
    try {
      val first = stream
        .filter(path => path.toString.toLowerCase.endsWith(".csv"))
        .findFirst()
      if (first.isPresent) Some(first.get().toString) else None
    } finally {
      stream.close()
    }
  }
}