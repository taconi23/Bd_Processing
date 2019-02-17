# Bd_Processing

## FASE 1

En este apartado debemos crear un código que lee el dataset en formato csv debemos realizar las siguientes transformaciones:

* Transformar el precio dado en dólarea a euros.
* Transformar el tamaño del piso dada en pies cuadrados ft<sup>2</sup> a metros cuadrado m<sup>2</sup>
* Transformar el precio por ft<sup>2</sup>  dado en dolares a euros por m<sup>2</sup>. Aquí hay dos formas de hacerlo volver aplicar 

## FASE 2
En la fase 1 nos dejaba un json en el directorio Real-estate con la localización y el precio medio en euros por m<sup>2</sup> en cada localización. Aquí tenemos que monitorizar ese directorio para que en el momento que el precio por m<sup>2</sup> supere un cierto límite enviar una alerta.

#### Pasos
 * Indicar el directorio a monitorizar. Esto se consigue con el código:
 ```
     val rawData = spark.readStream
        .format("json")
      .option("header", true)
      .schema(schema)
      .load("file:///home/kc/Documentos/datasets/real-estate/*")
 
 ```
 Con esta instrucción se indica que vamos a cargar "json", queremos que incluya la cabecera, le pasamos el esquema que se 
 obtiene con el código:
 
 ```
    val schema = new StructType()
      .add("Location", "string")
      .add("avg(PriceSQM2)", "double")
 ```
 y por último se cargar con la intrucción load pasando el *path*. Debido a que el programa de la fase 1 guarda los ficheros en carpetas  hay que poner el \* al final de todo para que procese los json dentro de subdirectorios.
 
 * El siguiente paso es parsear estos json para obtener los datos y procesarlos para lo cual tengo que hacer un flatMap que mapeará cada fila a una *case class* a través de un método. 
 
 ```
 
  def parseJson(x:Row): Option[Registros] = {

    if (x.getString(0) != ""){
      Some(Registros(x.getString(0), x.getDouble(1)))
    }
    else
      None
  }
 ```
 El método creado es muy sencillo ya que cada objeto de tipo Row que le llega lo convierte a la *case class* previamente creada  con los campos *Location* y *Precio*. Este precio es el precio en euros por metro cuadrado del inmueble.
 
 * Agrupar por localización y metro cuadrado. Esto se hace con:
  ```
      val windowedData2 = structuredData.groupBy($"Location").avg("Precio").orderBy($"avg(Precio)".desc)
  
  ```
  Que agrupa los datos por localización y los ordena en orden descendente. Con esta parte he estado también haciendo pruebas y metiendo una ventana temporal. creé otra varialble:
  
  ```
     val windowedData = structuredData.groupBy($"Location", window(current_timestamp(), "10 seconds"))
      .avg("Precio").orderBy($"avg(Precio)".desc)
  ```
  Hace lo mismo que la anterior solo que tenemos el tiempo por medio. Esto tiene implicaciones interesantes en el monitoreo ya que permite ver la secuencia temporal. Al meter el tiempo por medio lo que haría sería el agrupamiento y calculo de la media pero dentro del intervalo temporal mientras que la primera instrucción siempre agrupa y calcula la media. Por ejemplo he hecho dos pruebas:
  
  * **Prueba 1:** ventana temporal de 1 hora y realizo dos ingestas de datos diferentes. Los datos de cada variable son:
  
  ```
  -------------------------------------------
Batch: 0
-------------------------------------------
+-------------+--------------------+------------------+
|     Location|              window|       avg(Precio)|
+-------------+--------------------+------------------+
|Arroyo Grande|[2019-02-17 18:00...|32294.857699115048|
+-------------+--------------------+------------------+

-------------------------------------------
Batch: 0
-------------------------------------------
+-------------+------------------+
|     Location|       avg(Precio)|
+-------------+------------------+
|Arroyo Grande|32294.857699115048|
+-------------+------------------+

-------------------------------------------
Batch: 1
-------------------------------------------
+------------------+------------------+
|          Location|       avg(Precio)|
+------------------+------------------+
|     Arroyo Grande|17280.363663716817|
|        Los Alamos| 2054.495150442478|
|Santa Maria-Orcutt| 1823.831203539823|
|       Paso Robles|1667.2769203539824|
+------------------+------------------+

-------------------------------------------
Batch: 1
-------------------------------------------
+------------------+--------------------+------------------+
|          Location|              window|       avg(Precio)|
+------------------+--------------------+------------------+
|     Arroyo Grande|[2019-02-17 18:00...|32294.857699115048|
|     Arroyo Grande|[2019-02-17 19:00...| 2265.869628318584|
|        Los Alamos|[2019-02-17 19:00...| 2054.495150442478|
|Santa Maria-Orcutt|[2019-02-17 19:00...| 1823.831203539823|
|       Paso Robles|[2019-02-17 19:00...|1667.2769203539824|
+------------------+--------------------+------------------+

  ```
  En este caso tanto la variable que incorpora tiempo como la que no proporcionan los mismos resultados al pertenecer a la
  misma ventana temporal. 
  
  
  * **Prueba 2:** Ventana temporal de 10 segundos. Igual que antes realizo las dos mismas ingestas pero separadas por mas de 10 segundos. Los resultados de la primera y segunda ingesta son:
  
  ```
  -------------------------------------------
Batch: 0
-------------------------------------------
+-------------+------------------+
|     Location|       avg(Precio)|
+-------------+------------------+
|Arroyo Grande|32294.857699115048|
+-------------+------------------+

-------------------------------------------
Batch: 0
-------------------------------------------
+-------------+--------------------+------------------+
|     Location|              window|       avg(Precio)|
+-------------+--------------------+------------------+
|Arroyo Grande|[2019-02-17 18:47...|32294.857699115048|
+-------------+--------------------+------------------+

-------------------------------------------
Batch: 1
-------------------------------------------
+------------------+------------------+
|          Location|       avg(Precio)|
+------------------+------------------+
|     Arroyo Grande|17280.363663716817|
|        Los Alamos| 2054.495150442478|
|Santa Maria-Orcutt| 1823.831203539823|
|       Paso Robles|1667.2769203539824|
+------------------+------------------+
------------------------------------------
Batch: 1
-------------------------------------------
+------------------+--------------------+------------------+
|          Location|              window|       avg(Precio)|
+------------------+--------------------+------------------+
|     Arroyo Grande|[2019-02-17 18:47...|32294.857699115048|
|     Arroyo Grande|[2019-02-17 18:48...| 2265.869628318584|
|        Los Alamos|[2019-02-17 18:48...| 2054.495150442478|
|Santa Maria-Orcutt|[2019-02-17 18:48...| 1823.831203539823|
|       Paso Robles|[2019-02-17 18:48...|1667.2769203539824|
+------------------+--------------------+------------------+
  ```
  como podemos ver en la segunda prueba *Arroyo Grande* aparece duplicado y no se ha hecho el promedio temporal ya que está en ventanas diferentes. En cambio en la variable que no contenía el tiempo ha hecho la agrupación y media.
  
* Realizar la consulta y monitorización de los precios por si alguno se dispara por encima de un valor lo cual se hace con:

```
val query2 = windowedData2.coalesce(1).filter(line => line.getDouble(1)>2000).writeStream.outputMode("complete").foreach(new ForeachWriter[Row] {
      override def open(partitionId: Long, epochId: Long): Boolean = true

      override def process(value: Row): Unit = {
          println("Alerta " + value.getString(0) + " Valor medio superado:" + value.getDouble(1))

      }

      override def close(errorOrNull: Throwable): Unit = {}
    }).start

```

Realizo un filtro primero para quedarme con aquellos inmuebles que superen el valor límite y dentro del bloque process  se 
metería la lógica para el envío de e-mails.
