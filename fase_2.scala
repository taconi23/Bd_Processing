package bdproc.fase_2

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import bdproc.common.Utilities.setupLogging
import org.apache.spark.sql.types.StructType


object fase_2 {

  case class Registros(
                        Location: String,
                        Precio: Double
                      )

  def parseJson(x: Row): Option[Registros] = {

    if (x.getString(0) != "") {
      Some(Registros(x.getString(0), x.getDouble(1)))
    }
    else
      None
  }


  // Inicio del programa
  def main(args: Array[String]): Unit = {

    // configura la variable variable de alerta
    val alerta = 5000

    //session spark
    val spark = SparkSession
      .builder
      .appName("Agencia")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "file:///home/kc/Documentos/datasets/checkpoint")
      .getOrCreate()

    // configuración de los errores
    setupLogging()

    // estructura correspondientes a los csv
    val schema = new StructType()
      .add("Location", "string")
      .add("avg(PriceSQM2)", "double")

    import spark.implicits._

    // Carga de todos los csv existentes en el directorio
    val rawData = spark.readStream
      .format("json")
      .option("header", true)
      .schema(schema)
      .load("file:///home/kc/Documentos/datasets/real-estate/*")

    // Convierto los datos a datasets
    val structuredData = rawData.flatMap(parseJson).select("Location", "Precio")

    // Agrupación de datos con ventana temporal por localización y orden descendente
    val windowedData = structuredData.groupBy($"Location", window(current_timestamp(), "1 hour"))
      .avg("Precio").orderBy($"avg(Precio)".desc)

    // Agrupación de datos sin ventana temporal por localización y orden descendente
    val windowedData2 = structuredData.groupBy($"Location")
      .avg("Precio").orderBy($"avg(Precio)".desc)


    // realizo las consultas. query1 y query2 son consultas para mostar resultados en pantalla
    val query1 = windowedData.coalesce(1).writeStream.format("console").outputMode("complete").start

    val query3 = windowedData2.coalesce(1).writeStream.format("console").outputMode("complete").start

    val query2 = windowedData2.coalesce(1).filter(line => line.getDouble(1) > alerta).writeStream.outputMode("complete").foreach(new ForeachWriter[Row] {
      override def open(partitionId: Long, epochId: Long): Boolean = true

      override def process(value: Row): Unit = {

        // aqui se mete la lógica para envio de emails
        println("Alerta " + value.getString(0) + " Valor medio superado:" + value.getDouble(1))

      }

      override def close(errorOrNull: Throwable): Unit = {}
    }).start

    // Importante esperar a que termine la consulta
    query1.awaitTermination() // block until query is terminated, with stop() or with error
    query2.awaitTermination()
    query3.awaitTermination()


    spark.stop()
  }
}
