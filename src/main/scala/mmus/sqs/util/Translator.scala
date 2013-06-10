package mmus.sqs.util

import com.amazonaws.sqs.generated.Message
import com.amazonaws.sqs.generated.Attribute

object Translator {
    /**
     * Translates a list of JAXB-generated Messages into standard QMessages.
     * 
     * @param messages
     *            messages to translate
     * @return the translated messages
     * 
     * Scala version: Milosz M. 2013
     */
    def translateMessages(messages:List[Message]):List[QMessage] = {
        val newMessages = 
          for (message <- messages)
          yield {
              val temp = new QMessage(message.getMessageId(), message.getReceiptHandle(), message.getMD5OfBody(), message.getBody() )
              temp
          }

        newMessages;
    }

    /**
     * Translates a List of JAXB-generated Attributes into standard
     * QueueAttributes.
     * 
     * @param attributes
     *            attributes to translate
     * @return the translated attributes
     * 
     * Scala version: Milosz M. 2013
     */
    def translateAttributes(attributes:List[Attribute]): List[QueueAttribute] = {
        val newAttributes = 
          for (attribute <- attributes) yield {
              new QueueAttribute(attribute.getName(), attribute.getValue())
          }

        newAttributes;
    }

}