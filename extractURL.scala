/**
  * Created by Neta on 09/04/2016.
  */
import org.jsoup._
import org.apache.spark._
import scala.collection.JavaConversions._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeZone, DateTime}

object extractURL {

  def main(args: Array[String]) {

    val runTime: String = DateTime.now(DateTimeZone.UTC).toString(DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm"))

    val inputPath = ""
    val outputPath = ""

    val conf = new SparkConf().setMaster("local").setAppName("Works")
    val sc = new SparkContext(conf)

    val urls = sc.textFile(inputPath)
      .collect
      .toList

    val extractedUrls = urls.map(x => (x, extraction(x)))

    extractedUrls.take(10).foreach(x => println(x))
  }

  def extraction(url:String) : (List[String], (List[String]), (List[String])) ={

    try {
      val sourcesList = Jsoup.connect(if (url.startsWith("http://") || url.startsWith("https://")) url else "http://" + url)
        .followRedirects(false)
        .timeout(3000)
        .userAgent("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36")
        .referrer("http://www.google.com")
        .get()


      val rss = sourcesList
        .getElementsByAttributeValueMatching("type", "rss|atom")
        .iterator()
        .toList
        .map(x => x.attr("href"))
        .distinct
        .filter(x => !x.startsWith("/"))

      val fb = sourcesList
        .getElementsByAttributeValueContaining("href", "facebook.com")
        .iterator()
        .toList
        .map(x => x.attr("href"))
        .distinct
        .filter(x => !(x.contains("share") || x.contains("post") || x.contains("comment")))

      val tw =  sourcesList
        .getElementsByAttributeValueContaining("href", "twitter.com")
        .iterator()
        .toList
        .map(x => x.attr("href"))
        .distinct
        .filter(x => !(x.contains("intent") || x.contains("status")) && !x.contains("widgets"))

      return (rss, fb, tw)
    }
    catch {
      case _: Exception =>
        return (List(""), List(""), List(""))
    }
  }


}
