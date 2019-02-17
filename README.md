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
 
 
 
 * Agrupar por localización y metro cuadrado. Aqui se añade una ventana temporal que seguirá la evolución temporal del precio medio 
  de cada vivienda
  
 * 
