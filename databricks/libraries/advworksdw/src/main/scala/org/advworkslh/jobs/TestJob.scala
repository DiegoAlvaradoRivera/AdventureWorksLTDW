package org.advworkslh.jobs

object TestJob {

  def main(args: Array[String]): Unit = {
    val nameIndex = args.indexOf("--name")
    val name = if (nameIndex == -1) "User" else args(nameIndex+1)

    println(s"Hello $name")
  }

}
