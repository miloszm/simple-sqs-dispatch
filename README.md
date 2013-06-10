simple-sqs-dispatch
===================

Simple Scala SQS driver using Dispatch based on Amazon AWS Java SQSSample

Enter Amazon AWS credentials in SQSDriver.scala and do "sbt run".
Amazon SQS queue will be created, message will be sent to the created queue, then received and deleted.
