package mmus.sqs

import mmus.sqs.util.QMessage
import concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import java.util.concurrent.TimeoutException
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import scala.util.Success


/**
 * Scala version (experimental, under development): Milosz M. 2013
 */
object SQSDriver extends App {
  
	val accessKeyId = ""
	val secretAccessKey = ""
	val QueueServiceURL = "http://queue.amazonaws.com/"
	private val queueName = "SQS-Test-Queue-Scala"
	private val testMessage = "this is a test message send from Scala SQS driver"
	
	/**
	 *   responds with a "pong" after an x number of seconds
	 */  
  class DelayActor extends Actor {
    def receive = {
      case x:Int => context.system.scheduler.scheduleOnce(1000.millis*x, sender, "pong")
      case _ =>
    }
  }
	
	/**
	 *   responds with either error or queue
	 *   upon receiving a message with queue name  
	 */
	class CreateActor extends Actor {
    def receive = {
      case queueName:String => {
        val e = Queue.createQueue(queueName)
        if (e.isLeft){
          val err = e.left.get
          println("CreateQueue failed with error: %s".format(err.getErrorCode()))
          if (err.getErrorCode() ==  "AWS.SimpleQueueService.QueueDeletedRecently") {
              println("Recently Deleted Queue, wait 60 seconds");
              context.system.scheduler.scheduleOnce(60.seconds, self, queueName)
          } else {
            println(e)
            throw err // TODO
          }
        }
        else {
          sender ! e.right.get
        }
      }
    }
	}
	
  val system = ActorSystem("MyActorSystem")
  val delayActor:akka.actor.ActorRef = system.actorOf(Props[DelayActor], name = "delayActor")
  val createActor:akka.actor.ActorRef = system.actorOf(Props[CreateActor], name = "createActor")
  
  def delay(n:Int){
    val futureResponse = ask(delayActor, n)(Duration(n+1, SECONDS))
    Await.result(futureResponse, Duration(n+2, SECONDS))
  }
  
	println("Sample SQS Scala application")
  println("  - SQS WSDL 2008-01-01");
  println("testing delay of 3s");
  val interval = System.currentTimeMillis()
  delay(3)
  println("delay test ended in " + (System.currentTimeMillis() - interval) + "ms");
  
	
  if (accessKeyId == "" || secretAccessKey == "") {
    println("Please paste the values for your accessKey and your accessKeyId into the program before running the sample.");
    System.exit(1);
  }

  /**
   * experimental section begin
   * TODO - it will fail if returned value is not a success
   */
  val queueCreationResponse = ask(createActor, queueName)(Duration(120, SECONDS))
  Await.result(queueCreationResponse, Duration(120, SECONDS))
  val x:Option[_] = (for ( x <- queueCreationResponse.value ) yield {x})
  val y:Success[Queue] = x.toList(0).asInstanceOf[Success[Queue]]
  val testQueue:Queue = y.get
  /**
   * experimental section end
   */
  
  
  println("Looking for queue %s".format(testQueue.queueEndpoint))
  val f = future[Unit] { 
    def listAllMyQueues:Unit = {
      val queues = Queue.listQueues(queueName);
      val found = queues.exists(_.queueEndpoint == testQueue.queueEndpoint)
      if(!found) {
          println("Queue not available yet - keep polling\r");
          delay(10)
          listAllMyQueues
      }
    }
  }
  try {
    Await.result(f, Duration(40, SECONDS))
    println("queue " + testQueue.queueEndpoint + " has been found")
  }
  catch {
    case _:TimeoutException => throw new Exception("queue " + queueName + " could not be found")
  }
  
    
  // send a message
  val m = testQueue.sendMessage(testMessage)
  println("Message sent, message id: " + m.id);
  
    // Get Approximate Queue Count...
    // Since SQS is a distributed system, the count may not be accurate.
  val num = testQueue.getApproximateNumberOfMessages()
  println("Approximate Number of Messages: " + num)
  
  // now receive a message
  // because SQS is a distributed system, we need to poll until we get the message
  def receiveMessages():List[QMessage] = {
    val messages:List[QMessage] = testQueue.receiveMessage(1)
    messages match {
      case List() => delay(1);receiveMessages
      case x :: xs => messages 
    }
  }
  
  val message:QMessage = receiveMessages.head;

  println("")
  println("  Message received")
  println("  message id:      %s".format(message.id))
  println("  receipt handle:  %s".format(message.receiptHandle))
  println("  message content: %s".format(message.content))

  testQueue.deleteMessage(message.receiptHandle)
  println("Deleted the message.")
  
  system.stop(delayActor)
  system.shutdown

}