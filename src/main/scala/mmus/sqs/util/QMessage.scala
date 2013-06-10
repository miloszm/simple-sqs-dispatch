package mmus.sqs.util

/**
 * Scala version - Milosz M.
 */
case class QMessage(var id:String, var receiptHandle: String, var md5OfBody:String, var content:String)
