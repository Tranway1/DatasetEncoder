package edu.uchicago.cs.encsel.dataset.schema

import java.io.File

import org.junit.Assert._
import org.junit.Test

import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.dataset.schema.Schema;
import scala.io.Source

class SchemaTest {
  @Test
  def testGetSchema(): Unit = {
    var schema = Schema.getSchema(new File("src/test/resource/find_schema.csv").toURI())
    assertEquals(3, schema.columns.length)

    schema = Schema.getSchema(new File("src/test/resource/find_schema2.tsv").toURI())
    assertEquals(6, schema.columns.length)

    schema = Schema.getSchema(new File("src/test/resource/fuzzy_find_schema_3.csv").toURI())
    assertEquals(5, schema.columns.length)
  }

  @Test
  def testToSchema: Unit = {
    var schema = new Schema()
    schema.columns = Array((DataType.INTEGER, "Wa"), (DataType.DOUBLE, "Ma"), (DataType.STRING, "Ka"))
    schema.hasHeader = true

    Schema.toParquetFile(schema, new File("src/test/resource/test_to_schema.schema.gen").toURI())

    var origin = Source.fromFile(new File("src/test/resource/test_to_schema.schema")).getLines().toArray
    var gened = Source.fromFile(new File("src/test/resource/test_to_schema.schema.gen")).getLines().toArray

    assertArrayEquals(gened.toArray[Object], origin.toArray[Object])
  }
}