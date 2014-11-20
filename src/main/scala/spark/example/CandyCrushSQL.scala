package spark.example

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST._
import org.json4s.DefaultFormats

/**
 * This example illustrates how you can use Apache Spark to process JSON messages
 * stored in a file, in this case we acquired the data from Datasift (www.datasift.com)
 */
object CandyCrushSQL {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "CandyCrushSQL")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._

    
    // We want to extract the level number from "Yay, I completed level 576 in Candy Crush Saga!"
    // the actual text will change based on the users language but parsing the 'last number' works
    val pattern = """(\d+)""".r
    
    
    // Produces a RDD[String]
    val lines = sc.textFile("data/facebook-2014-05-19.json")
    
    
    // Process the messages
    val interactions = lines.map(line => {
      // Parse the JSON, returns RDD[JValue]
      parse(line)
    }).filter(json => {
      // Filter out only 'Candy Crush Saga' Facebook App activity
      get(json \ "facebook" \ "application") == "Candy Crush Saga"
    }).map(json => {
      // Extract fields we want, we use compact() because they may not exist
      val id = get(json \ "facebook" \ "id").toString
      val user = get(json \ "facebook" \ "author" \ "hash").toString
      val gender = get(json \ "demographic" \ "gender").toString
      val language = get(json \ "language" \ "tag").toString
      // Extract the 'level' using a RegEx or default to zero
      var level = 0;
      pattern.findAllIn( get(json \ "interaction" \ "title").toString ).matchData.foreach(m => {
        level = m.group(1).toInt
      })
      // Return an RDD[CandyCrushInteraction] 
      ( CandyCrushInteraction(id, user, level, gender, language) )
    })
    
    // Now we register the RDD[CandyCrushInteraction] as a Table
    interactions.registerTempTable("candy_crush_interaction")
    
    // Game level by Gender
    println("Game level by Gender:\n=====================")
    sql("""
      SELECT gender, COUNT(level), MAX(level), MIN(level), AVG(level)
      FROM candy_crush_interaction
      WHERE level > 0 GROUP BY gender
    """.stripMargin).collect().foreach(println)
    
    /* Example output shows that 'female' users have a high average reported level compared to 'male' users
     */
    /*   gender          count, max, min, avg
        ["male",         14727, 590, 1, 104.7]
        ["female",       15422, 590, 1, 114.2]
        ["mostly_male",   2824, 590, 1, 97.1]
        ["mostly_female", 1934, 590, 1, 99.1]
        ["unisex",        2674, 590, 1, 113.4]
     */

    // Game level by Language
    println("Game level by Language:\n=======================")
    sql("""
      SELECT language, COUNT(level), MAX(level), MIN(level), AVG(level)
      FROM candy_crush_interaction
      WHERE level > 0 GROUP BY language
    """.stripMargin).collect().foreach(println)
    
    /* Example output shows that German (de) users have a high average reported
     * level compared to English, French or Spanish.  Although the sample of users
     * from Germany is low (819) compared to English (24256).
     */
    /*  lang   count, max, min, avg
        ["de",   819, 590, 1, 137.8]
        ["fr",  5188, 590, 1, 137.5]
        ["es",  6833, 590, 1, 109.4]
        ["en", 24256, 590, 1, 91.5]
     */
    
    sc.stop()
  }
  
  // Utility method to unpack the value from JSON
  def get(value: JValue) = {
    value match {
      case JNothing | JNull => ""
      case JString(s)    => s
      case JDouble(num)  => num
      case JDecimal(num) => num
      case JInt(num)     => num
      case JBool(value)  => value
      case _ => ""
    }
  }

}