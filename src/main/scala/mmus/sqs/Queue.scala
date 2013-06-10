package mmus.sqs

import com.amazonaws.sqs.generated.CreateQueueResponse;
import com.amazonaws.sqs.generated.DeleteMessageResponse;
import com.amazonaws.sqs.generated.DeleteQueueResponse;
import com.amazonaws.sqs.generated.ErrorResponse;
import com.amazonaws.sqs.generated.Error;
import com.amazonaws.sqs.generated.GetQueueAttributesResponse;
import com.amazonaws.sqs.generated.ListQueuesResponse;
import com.amazonaws.sqs.generated.ReceiveMessageResponse;
import com.amazonaws.sqs.generated.SendMessageResponse;
import com.amazonaws.sqs.generated.SetQueueAttributesResponse;
import mmus.sqs.util.QMessage;
import com.amazonaws.sqs.generated.Message
import mmus.sqs.util.QueueAttribute;
import com.amazonaws.sqs.util.QueueException;
import mmus.sqs.util.Translator;
import scala.collection.JavaConverters._


/**
 * Scala version of the original Java code from AWS: Milosz M. 2013
 */
class Queue(val queueEndpoint:String) {
  
    private val ApproximateNumberOfMessagesAttr = "ApproximateNumberOfMessages"
    private val VisibilityTimeoutAttr = "VisibilityTimeout"
    private val AllAttr = "All"

    def deleteMessage(receiptHandle:String):Unit = {
        val sqsClient = SQSClient(queueEndpoint);
        sqsClient.setAction("DeleteMessage");
        if (receiptHandle != null) {
            sqsClient.addQueryParam("ReceiptHandle", receiptHandle);
        }

        val response:Object = sqsClient.callAndUnmarshal
        val e = Queue.respToEither(response, response.isInstanceOf[DeleteMessageResponse])
        if (e.isLeft){
          throw e.left.get
        }
        //val dmr = e.right.get.asInstanceOf[DeleteMessageResponse]
    }

    def deleteQueue():Unit = {
        val sqsClient = SQSClient(queueEndpoint);
        sqsClient.setAction("DeleteQueue");

        val response:Object = sqsClient.callAndUnmarshal
        val e = Queue.respToEither(response, response.isInstanceOf[DeleteQueueResponse])
        if (e.isLeft){
          throw e.left.get
        }
        //val dmr = e.right.get.asInstanceOf[DeleteQueueResponse]
    }

    def receiveMessage(numMessages: Int):List[QMessage] = {
        val sqsClient = SQSClient(queueEndpoint);
        sqsClient.setAction("ReceiveMessage");
        if (numMessages != 0) {
            sqsClient.addQueryParam("MaxNumberOfMessages", String.valueOf(numMessages));
        }

        val response:Object = sqsClient.callAndUnmarshal
        val e = Queue.respToEither(response, response.isInstanceOf[ReceiveMessageResponse])
        if (e.isLeft){
          throw e.left.get
        }
        else {
          var rmr = e.right.get.asInstanceOf[ReceiveMessageResponse]
          // parse results
          if (rmr.getReceiveMessageResult() != null)
              Translator.translateMessages(rmr.getReceiveMessageResult().getMessage().asScala.toList);
          else
              List[QMessage]()
        }
    }

    def sendMessage(messageBody:String):QMessage = {
        val sqsClient = SQSClient(queueEndpoint);
        sqsClient.setAction("SendMessage");
        if (messageBody != null) {
            sqsClient.addQueryParam("MessageBody", messageBody);
        }

        val response:Object = sqsClient.callAndUnmarshal
        val e = Queue.respToEither(response, response.isInstanceOf[SendMessageResponse])
        if (e.isLeft){
          throw e.left.get
        }
        else {
          val smr = e.right.get.asInstanceOf[SendMessageResponse]
          val result = new QMessage(smr.getSendMessageResult().getMessageId(), "", smr.getSendMessageResult().getMD5OfMessageBody(), messageBody)
          result
        }
    }

    def getQueueAttributes(attributes:List[QueueAttribute]):List[QueueAttribute] = { 
        val sqsClient = SQSClient(queueEndpoint);
        sqsClient.setAction("GetQueueAttributes");
        if (attributes != null) {
            for (attribute <- attributes) {
                sqsClient.addQueryParam("AttributeName", attribute.name);
            }
        }

        val response = sqsClient.callAndUnmarshal
        val e = Queue.respToEither(response, response.isInstanceOf[GetQueueAttributesResponse])
        if (e.isLeft){
          throw e.left.get
        }
        else {
          val gqar = e.right.get.asInstanceOf[GetQueueAttributesResponse]
          Translator.translateAttributes(gqar.getGetQueueAttributesResult().getAttribute().asScala.toList);
        }
        
    }

    def setQueueAttributes(attributes:List[QueueAttribute]):Unit = {
        val sqsClient = SQSClient(queueEndpoint)
        sqsClient.setAction("SetQueueAttributes");
        if (attributes != null) {
            for (attribute <- attributes) {
                sqsClient.addQueryParam(attribute.name, attribute.value);
            }
        }
        val response = sqsClient.callAndUnmarshal
        val e = Queue.respToObject(response)
        
        if (e.isLeft) 
          throw e.left.get
    }

    def getApproximateNumberOfMessages():String = {
        val countAttr = new QueueAttribute(ApproximateNumberOfMessagesAttr, "");

        val result = getQueueAttributes(List[QueueAttribute](countAttr))
        val attributes: List[QueueAttribute] = result
        attributes.head.value
    }

}

object Queue {
  
def createQueue(name: String):Either[QueueException,Queue] = {
        val sqsClient:SQSClient = SQSClient(SQSDriver.QueueServiceURL);

        sqsClient.setAction("CreateQueue");
        if (name != null) {
            sqsClient.addQueryParam("QueueName", name);
        }

        val response:Object = sqsClient.callAndUnmarshal
        val e = respToEither(response, response.isInstanceOf[CreateQueueResponse])
        if (e.isLeft){
            Left(e.left.get)
        }
        else {
          val cqr = e.right.get.asInstanceOf[CreateQueueResponse]
          val newQueue = new Queue(cqr.getCreateQueueResult().getQueueUrl())
          val qe = newQueue.queueEndpoint
          println("Created a new queue.  Queue url: %s2".format(qe))
          Right(newQueue)
        }
    }
    
    def listQueues(prefix:String):List[Queue] = {
        val sqsClient = SQSClient(SQSDriver.QueueServiceURL);

        sqsClient.setAction("ListQueues");
        if (prefix != null && !prefix.equals("")) {
            sqsClient.addQueryParam("QueueNamePrefix", prefix);
        }

        val response:Object = sqsClient.callAndUnmarshal
        
        val e = respToEither(response, response.isInstanceOf[ListQueuesResponse])
        if (e.isLeft){
            throw e.left.get
        }
        else {
          val lqr = e.right.get.asInstanceOf[ListQueuesResponse]
          val queues = 
            if (lqr.getListQueuesResult() != null && lqr.getListQueuesResult().getQueueUrl() != null) {
              val ss:List[String] = (lqr.getListQueuesResult().getQueueUrl()).asScala.toList
              for (queueUrl:String <- ss) yield (new Queue(queueUrl))
            }
            else {
              List[Queue]()
            }
          queues
       }
    }
    
    /**
     * converts error response to a queue exception
     */
    private def errorResponseToQueueException(response:Object):QueueException = {
      val errorResponse = response.asInstanceOf[ErrorResponse]
      val error:Error = errorResponse.getError().get(0)
      val qException:QueueException = new QueueException(error.getCode())
      qException
    }
    
    /**
     * wraps response with 'either'
     */
    private def respToEither(response:AnyRef, sameClass:Boolean):Either[QueueException,AnyRef] = {
      if (sameClass) Right(response) else Left(errorResponseToQueueException(response))
    }
    
    /**
     * wraps response with 'either'
     */
    def respToObject(response:AnyRef):Either[QueueException,AnyRef] = {
      if (response.isInstanceOf[ErrorResponse]) Left(errorResponseToQueueException(response))
      else Right(response)
    }
    
  
}