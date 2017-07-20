import org.apache.commons.codec.language.Metaphone


object CallMetaPhone {
  
//  metaphone setMaxCodeLen 5

  def encode(str:String) : String = {
    val metaphone = new Metaphone
    metaphone encode str
  }
  
  def main(args: Array[String]) {
    println(CallMetaPhone encode "test")
  }
}