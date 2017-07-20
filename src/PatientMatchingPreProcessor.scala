import scala.io.Source 

object PatientMatchingPreProcessor {
    def main(args: Array[String]) {
      val bufferedSource = Source.fromFile("/home/paul/cleandata.csv")
      for (line <- bufferedSource.getLines) {
        println(line)
//        if (line.isEmpty()) 
          val cols = line.split(",").map(_.trim)
//
//          println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
      }
      bufferedSource.close
    }
}