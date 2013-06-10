package mmus.sqs
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.SimpleTimeZone
import com.amazonaws.sqs.util.AwsSignature

/**
 * code independent of the HTTP client used
 */
trait SQSClient {

  val ProtocolVersion = "2008-01-01"
  val SignatureVersion = "1"
      
  def setAction(action:String)
  def addQueryParam(paramName:String, paramValue:String)
  def getQueryParams:Map[String,String]
  def callAndUnmarshal:AnyRef
  
  def getExpires():String = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.MINUTE, 60)

    val timeZone = new SimpleTimeZone(0, "PST")

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssz")
    format.setTimeZone(timeZone);
    val timestampStr = format.format(cal.getTime())
    timestampStr
  }

  def calculateStringToSign(): String = {
    val keys = getQueryParams.keySet.toList.sortWith(_.toUpperCase < _.toUpperCase)
    val list:List[String] = for (k <- keys; i <- 0 to 1 ) yield {if (i == 0) k else getQueryParams(k)}
    list mkString ""
  }
    
  def calculateSignature(stringToSign:String):String = {
    AwsSignature.calculate(stringToSign, SQSDriver.secretAccessKey);
  }
  
}

/**
 * provides default implementation based on Dispatch
 */
object SQSClient {
  def apply(endpoint:String) = {
    new mmus.sqs.util.SQSDispatchClient(endpoint)
  }
}

