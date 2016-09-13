package com.cars.bigdata.turbocow.utils

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Files

import scala.io.Source

object FileUtil {

  /** Helper function to quickly read in a file.
    */
  def fileToString(filePath: String) = Source.fromFile(filePath).getLines.mkString

  /** Helper function to quickly write a file to a temp dir.  Returns the file path.
    */
  def writeTempFile(text: String, nameOfFile: String): String = {
    val dir = Files.createTempDirectory("testoutput-")
    val path = dir + "/" + nameOfFile
    val bw = new BufferedWriter(new FileWriter(new File(path)))
    bw.write(text)
    bw.close()
    path
  }


  /** write out a file.  the file is overwritten.
    */
  def writeFile(text: String, filename: String) = {
    val outputFile = new java.io.File(filename)
    outputFile.delete()
    val bw = new java.io.BufferedWriter(new java.io.FileWriter(outputFile))
    bw.write(text)
    bw.close()
  }

}

