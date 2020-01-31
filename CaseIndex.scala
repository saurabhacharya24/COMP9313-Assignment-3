import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import play.api.libs.json._
import org.json._
import scalaj.http._
import java.io._
import java.util._

// To run the application use the following command (after running 'sbt package'):
// spark-submit --class "CaseIndex" --packages org.scalaj:scalaj-http_2.11:2.4.2,org.json:json:20180813,com.typesafe.play:play-json_2.11:2.7.4 --master local[2] JAR_FILE FULL_PATH_OF_DIRECTORY_WITH_CASE_FILES

object CaseIndex {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("CaseIndex").setMaster("local")
    val sc = new SparkContext(conf)

    val inputDir = new File(args(0))

    // Filtering only XML files
    val files = inputDir.listFiles.filter(_.getName.endsWith(".xml")).toList

    val h1 = "content-type"
    val h2 = "application/json"
    val es_url = "http://localhost:9200/"
    val nlp_url = "http://localhost:9000/"

    // Index creation
    Http(es_url + "legal_idx")
        .method("put")
        .asString

    val mapping = """{
                    "mapping": {
                      "cases": {
                        "properties": {
                          "name": {
                            "type": "text"
                          }
                          "AustLII": {
                            "type": "text"
                          }
                          "catchphrases": {
                            "type": "object"
                            "properties": {
                                "catchphrase": {
                                    "type": "text"
                                }
                            }
                          }
                          "sentences": {
                            "type": "object"
                            "properties": {
                                "id": {
                                    "type": "text"
                                }
                                "content": {
                                    "type": "text"
                                }
                            }
                          }
                          "person": {
                            "type": "text"
                          }
                          "location": {
                            "type": "text"
                          }
                          "organization": {
                            "type": "text"
                          }                         
                        }
                      }
                    }
                  }"""

    // Applying mapping to the index
    Http(es_url + "legal_idx/")
        .postData(mapping)
        .header(h1, h2)
        .method("put")
        .asString

    // Parsing each file's content and adding document into index
    var i: Int = 1
    for (f <- files) {
        val counter: String = i.toString
        var fileContents = Source.fromFile(f, "ISO-8859-1").getLines.mkString
        val jsonified: JSONObject = XML.toJSONObject(fileContents)
        val jsonCase = Json.parse(jsonified.toString)
        val currCase = jsonCase \\ "case"
        val currCaseString: String = currCase.mkString
        
        Http(es_url + "legal_idx/cases/case_" + counter + "/")
                .postData(currCaseString)
                .header(h1, h2)
                .method("put")
                .asString

        i += 1
    }

    // Http post request sent again to combat type inconsistency errors with ElasticSearch
    var j: Int = 1
    for (f <- files) {
        println("Adding case_" + j + " to legal_idx...")

        val counter: String = j.toString
        val fileContents = Source.fromFile(f, "ISO-8859-1").getLines.mkString
        val jsonified: JSONObject = XML.toJSONObject(fileContents)
        val jsonString: String = jsonified.toString
        val jsonCase = Json.parse(jsonified.toString)
        val currCase = jsonCase \\ "case"
        val currCaseString: String = currCase.mkString

        // Extracting necessary words to send to NLP server
        var nlpJsonString: String = jsonString.replaceAll("<[a-z]>", "")
        val nlpJsonArray = nlpJsonString.replaceAll("[^a-zA-Z0-9 ]", " ").split(" ")
        val finalNlpArray = nlpJsonArray.distinct.filter(_.matches("^[A-Z].*"))
        
        // Adding document to index
        Http(es_url + "legal_idx/cases/case_" + counter + "/")
                .postData(currCaseString)
                .header(h1, h2)
                .method("put")
                .asString

    	var personList = Array.empty[String]
        var locationList = Array.empty[String]
        var orgList = Array.empty[String]	
    	
        // Iterating through words to send to NLP server
        // (Since sending the entire document to NLP server is too time-taking)
    	for (nlpWord <- finalNlpArray) {
    		val nlpWordString = nlpWord.mkString
    		val res = Http(nlp_url + "?properties=%7B%22annotators%22:%22ner%22,%22outputFormat%22:%22json%22%7D")
    		            .postData(nlpWordString)
    		            .method("POST")
    		            .header(h1, h2)
    		            .asString

    		val response = Json.parse(res.body)
    		val words = response \ "sentences" \ 0 \ "tokens" // Actual list of word objects

    		val word = (words \ 0 \ "word").as[String]
    		val ner = (words \ 0 \ "ner").as[String]

    		// Checking type of word (person, location, organization) and inserting
    		// into appropriate array
    		if (ner == "PERSON") { personList = personList :+ s""""$word""""}
    		else if (ner == "LOCATION") { locationList = locationList :+ s""""$word""""}
    		else if (ner == "ORGANIZATION") { orgList = orgList :+ s""""$word""""}
        }
    	
        // Converting array to appropriate string format
        // since updating documents takes in a formatted JSON object in the body
        val personString = personList.mkString(",")
        val locationString = locationList.mkString(",")
        val orgString = orgList.mkString(",")

        // Update-body containing personList, locationList and orgList
        val updateDoc = s"""{
                             "doc": {
                                 "person": [$personString],
                                 "location": [$locationString],
                                 "organization": [$orgString]
                             }
                            }"""

        // Updating document with persons, locations, and organizations
        Http(es_url + "legal_idx/cases/case_" + counter + "/_update")
            .postData(updateDoc)
            .header(h1, h2)
            .method("post")
            .asString

        println("case_" + j + " has been added to legal_idx.")
        j += 1
    }
  }
}
