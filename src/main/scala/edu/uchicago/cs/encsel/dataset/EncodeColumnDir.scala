package edu.uchicago.cs.encsel.dataset

import java.net.URI
import edu.uchicago.cs.encsel.model._
import edu.uchicago.cs.encsel.adapter.parquet.ParquetWriterHelper
import edu.uchicago.cs.encsel.dataset.EncodeSingleColumnDir.{extractDataType, mapDataType}
import java.io.{BufferedReader, FileReader}
import java.net.URI
import java.util.logging.{FileHandler, Level, Logger, SimpleFormatter}

object EncodeColumnDir {
  def encode(dir: URI, dataType: DataType) = {
    dataType match {
      case DataType.STRING => {
        StringEncoding.values().filter(_.parquetEncoding() != null).take(2).foreach(e =>
          try {
            ParquetWriterHelper.singleColumnString(dir, e)
          } catch {
            case e: IllegalArgumentException => {
              e.printStackTrace()
            }
          }
        )
      }
      case DataType.LONG => {
        LongEncoding.values().filter(_.parquetEncoding() != null).take(2).foreach(e =>
          try {
            ParquetWriterHelper.singleColumnLong(dir, e)
          } catch {
            case e: IllegalArgumentException => {
              e.printStackTrace()
            }
          }
        )
      }
      case DataType.INTEGER => {
        IntEncoding.values().filter(_.parquetEncoding() != null).take(2).foreach(e =>
          try {
            ParquetWriterHelper.singleColumnInt(dir, e)
          } catch {
            case e: IllegalArgumentException => {
              e.printStackTrace()
            }
          }
        )
      }
      case DataType.FLOAT => {
        FloatEncoding.values().filter(_.parquetEncoding() != null).take(2).foreach(e =>
          try {
            ParquetWriterHelper.singleColumnFloat(dir, e)
          } catch {
            case e: IllegalArgumentException => {
              e.printStackTrace()
            }
          }
        )
      }
      case DataType.DOUBLE => {
        FloatEncoding.values().filter(_.parquetEncoding() != null).take(2).foreach(e =>
          try {
            ParquetWriterHelper.singleColumnDouble(dir, e)
          } catch {
            case e: IllegalArgumentException => {
              e.printStackTrace()
            }
          }
        )
      }
      case DataType.BOOLEAN => {}
    }
  }
}



object EncodeSingleColumnDir extends App {
  def extractDataType(filename: String): String = {
    val pattern = """.*_([a-z]+)\.txt$""".r
    filename match {
      case pattern(dataType) => dataType
      case _ => throw new IllegalArgumentException("Invalid file name format")
    }
  }

  def mapDataType(t: String): DataType = {
    t.toLowerCase match {
      case "integer" | "smallint" => DataType.INTEGER
      case "bigint" => DataType.LONG
      case "double" | "decimal" => DataType.DOUBLE
      case "float" => DataType.FLOAT
      case _ => DataType.STRING
    }
  }

  val dir = args(0)
  val t = extractDataType(dir)
  val dataType = mapDataType(t)
  val absoluteDir = if (dir.startsWith("file://")) dir else s"file://$dir"
  val uri = new URI(absoluteDir)
  EncodeColumnDir.encode(uri, dataType)
}


object EncodeMultipleFiles extends App {
  val logger = Logger.getLogger("EncodingProgress")
  val fileHandler = new FileHandler("/job/encoding_progress.log", true)
  fileHandler.setFormatter(new SimpleFormatter())
  logger.addHandler(fileHandler)
  logger.setLevel(Level.INFO)
  val inputFile = args(0)

  val fileNames = scala.io.Source.fromFile(inputFile).getLines().toList

  fileNames.foreach { fileName =>
    val extractedType = extractDataType(fileName)
    val dataType = mapDataType(extractedType)
    val absoluteDir = if (fileName.startsWith("file://")) fileName else s"file:///job/columns/$fileName"
    val uri = new URI(absoluteDir)
    logger.info(s"Encoding file: $fileName")
    EncodeColumnDir.encode(uri, dataType)
  }
}
