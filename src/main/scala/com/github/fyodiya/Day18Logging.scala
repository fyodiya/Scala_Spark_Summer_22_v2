package com.github.fyodiya

import org.apache.log4j.Logger
import org.apache.logging.log4j.{Level}

object Day18Logging extends App {

  //logging in JVM world is done through the log4j library
  println(classOf[Day18Logging].getName)
  val log = Logger.getLogger(classOf[Day18Logging].getName) //considered a good practice to assign classname to particular
  //instead of println we would use this log.method

  log.debug("Hello, this is a debug message.")
  log.info("Hello, this is an info message.")
  log.warn("This is a warning.")
  log.error("This is an error!")

  //there are more wrapper libraries such as Logback
  //https://mvnrepository.com/artifact/ch.qos.logback/logback-classic

}

class Day18Logging
//an empty class just to give name to our Logger, could have used a string
