package mmus.sqs.util

import dispatch._
import scala.concurrent.ExecutionContext
import javax.xml.bind.JAXBContext
import javax.xml.bind.JAXBException
import javax.xml.bind.Unmarshaller
import java.io.StringReader
import mmus.sqs._
import scala.concurrent.duration._
import scala.concurrent.Await
import ExecutionContext.Implicits.global


/**
 * Scala Dispatch based SQS client
 *  
 * equivalent to AWS Java Apache HTTP Client based SQSClient
 * 
 * Milosz M. 2013 
 */
class SQSDispatchClient(endpoint:String) extends SQSClient {
  
  val u = url(endpoint)
  val jaxbContext = JAXBContext.newInstance("com.amazonaws.sqs.generated");
  var queryParams:Map[String,String] = Map()
  
  def setAction(action:String) = {
    addQueryParam("Action", action)
  }  

  def addQueryParam(paramName:String, paramValue:String) = {
    u.addQueryParameter(paramName, paramValue)
    queryParams += (paramName -> paramValue)
  }

  def getQueryParams:Map[String,String] =
    queryParams
  
  def callAndUnmarshal:AnyRef = {
    addQueryParam("Expires", getExpires)
    addQueryParam("AWSAccessKeyId", SQSDriver.accessKeyId)
    addQueryParam("Version", ProtocolVersion)
    addQueryParam("SignatureVersion", SignatureVersion)
    addQueryParam("Signature", calculateSignature(calculateStringToSign()));
    
    val promise = Http(u OK as.String)
    
    val p:String = Await.result(promise, Duration(5, SECONDS))
    
    val unmarshaller = jaxbContext.createUnmarshaller()
    unmarshaller.unmarshal(new StringReader(p))
  }
  
}
