package com.github.fyodiya

import scala.language.postfixOps

object Day37CommandLineArguments extends App {

  println("Command line arguments")

  //we have arguments supplied to us by the main method (which we get for free by Extend App)
  for ((arg, i) <- args.zipWithIndex) {
    println(s" argument No. $i, argument: $arg")
  }
  //you can check for length
  if (args.length >= 1) {
    println()
  }

  //so we have some default which would work and also option to supply command line argument
  val defaultSrc = "src/resources/csv/mydefaultcsv.csv"
  //so our src will either be default file  or the first argument supplied by user
  val src = if (args.length >= 1) args(0) else defaultSrc

  println(s"My Src file will be $src")

}
