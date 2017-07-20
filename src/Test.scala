import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.io.File

object Test {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("my-spark-app")
      .setMaster("local")
    val sc = new SparkContext(conf)
    
    val spark = SparkSession.builder
    .master("local")
    .appName("my-spark-app")
    .config("spark.some.config.option", "config-value")
    .getOrCreate()
  
   
    val df = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/paul/Downloads/FInalDataset.csv")

    val df2 = df.withColumn("enternew", df("EnterpriseID").cast("integer"))
    .drop("EnterpriseID").withColumnRenamed("enternew", "EnterpriseID")
    
    val df3 = df2.withColumn("zipnew", df2("ZIP").cast("integer"))
    val df4 = df3.drop("ZIP").withColumnRenamed("zipnew", "ZIP")
    
    
    df.printSchema()

    df.createOrReplaceTempView("patient")
    
    val idnotnull = spark.sql("select * from patient where EnterpriseID is not null")
    
    idnotnull.createOrReplaceTempView("idnotnull")
    
    val nullssn = spark.sql("select * from idnotnull where LAST is null and FIRST is null and SSN is null")
    
    nullssn.createOrReplaceTempView("nullssn")
    
    val notnullssn = spark.sql("select * from idnotnull where EnterpriseID not in (select EnterpriseID from nullssn)")
    
//    val update = notnullssn.withColumn("GENDERNEW", regexp_replace(notnullssn("GENDER"), "FEMALE", "F"))
    
//    val update1 = update.withColumn("GENDERNEW2", regexp_replace(update("GENDERNEW"), "MALE", "M"))
    
//    val update2 = update1.drop("GENDER")
//    
//    val update3 = update2.drop("GENDERNEW")
//    
//    val update4 = update3.withColumnRenamed("GENDERNEW2", "GENDER")
//    
//    
    
    val df2 = df.withColumn("FIRSTP", udf(encode _).apply(df("FIRST")))
    
//    
//    update4.createOrReplaceTempView("update4")
//    
//    update4.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/paul/cleandata")
//    
////    df5.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/paul/cleandata")
//    
//    val df1 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/paul/clean.csv")
//
//    
//    spark.sql("select * from update4").count()
////    
//    spark.sql("select p1.EnterpriseID, p2.EnterpriseID, p1.LAST, p1.FIRST, p1.SSN, p1.DOB, p2.DOB from notnullssn p1, notnullssn p2 where p1.LAST=p2.LAST and p1.FIRST=p2.FIRST and p1.LAST is not null and p1.FIRST is not null and p1.SSN = p2.SSN and p1.EnterpriseID <> p2.EnterpriseID order by p1.LAST, p1.FIRST").show()

    new File("/home/paul/cleandata/part-00000-66c0c067-1eea-41b5-9a1d-965dc68775b2.csv").renameTo(new File("home/paul/cleandata2.csv"))
    
    val df = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/paul/cleandata2.csv")
    
    val df2 = df.withColumn("DOBdate", to_date(df("DOB")))
    
    val df5 = spark.sql("select p1.EnterpriseID, p2.EnterpriseID id2, p1.LAST, p2.LAST as last2,p1.FIRST, p2.FIRST as first2, p1.SSN, p1.DOBdate, p2.DOBdate as date2, p1.LASTP, p2.LASTP lastp2,p1.FIRSTP, p2.FIRSTP firstp2, p1.GENDER, p2.GENDER gender2, p1.ZIP, p2.ZIP as zip2 from df2 p1, df2 p2 where p1.SSN is not null and p2.SSN is not null and p1.SSN=p2.SSN and p1.EnterpriseID <> p2.EnterpriseID")
    
    val df6 = df5.withColumn("lastdistance", levenshtein($"LAST", $"last2"))

    val df7 = df6.withColumn("firstistance", levenshtein($"FIRST", $"first2"))
    
      val df8 = df7.withColumn("lastsounddistance", levenshtein($"LASTP", $"lastp2"))
      
      val df9 = df8.withColumn("firstsounddistance", levenshtein($"FIRSTP", $"firstp2"))
    
      val df10 = df9.withColumn("datedistance", abs(datediff($"DOBdate", $"date2")))
      
      val df11 =  df10.dropDuplicates({"SSN"})
      
    df10.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/paul/cleandata")
    
    val df = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/paul/firstcompare.csv")

    
   }
  
  def get_key(x: ColumnName) : String = { x("SSN") }
//  https://stackoverflow.com/questions/3183149/most-efficient-way-to-calculate-levenshtein-distance
//  def levenshtein(s: CharSequence, t: CharSequence, max: Int = Int.MaxValue) = {
//import scala.annotation.tailrec
//def impl(s: CharSequence, t: CharSequence, n: Int, m: Int) = {
//  // Inside impl n <= m!
//  val p = new Array[Int](n + 1) // 'previous' cost array, horizontally
//  val d = new Array[Int](n + 1) // cost array, horizontally
//
//  @tailrec def fillP(i: Int) {
//    p(i) = i
//    if (i < n) fillP(i + 1)
//  }
//  fillP(0)
//
//  @tailrec def eachJ(j: Int, t_j: Char, d: Array[Int], p: Array[Int]): Int = {
//    d(0) = j
//    @tailrec def eachI(i: Int) {
//      val a = d(i - 1) + 1
//      val b = p(i) + 1
//      d(i) = if (a < b) a else {
//        val c = if (s.charAt(i - 1) == t_j) p(i - 1) else p(i - 1) + 1
//        if (b < c) b else c
//      }
//      if (i < n)
//        eachI(i + 1)
//    }
//    eachI(1)
//
//    if (j < m)
//      eachJ(j + 1, t.charAt(j), p, d)
//    else
//      d(n)
//  }
//  eachJ(1, t.charAt(0), d, p)
//}
//
//val n = s.length
//val m = t.length
//if (n == 0) m else if (m == 0) n else {
//  if (n > m) impl(t, s, m, n) else impl(s, t, n, m)
//}
//}
//}