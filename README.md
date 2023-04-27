# Spark convertir jason a formato tabular
Usando PYSPARK, convertir  JSON "airports.json" del CP M4 con formato STRUCT a formato TABULAR

# Contenido
- cp_m4_json_explode_miguel.ipynb : notebookk con el codigo fuente
- airports.json : fuente de datos
 
# Explicacion del codigo
Usando Spark, creamos el dataframe
```
df1 = spark.read.json(path= "dbfs:/FileStore/shared_uploads/tacnampt@gmail.com/airports-3.json")
df1.show(2)
df1.printSchema()
```
Podemos observar que "airport_id: struct", es tipo struct, no hay filas, toda esta concentrado en una celda.
```
+--------------------+--------------------+--------------------+--------------------+
|          airport_id|                city|                name|               state|
+--------------------+--------------------+--------------------+--------------------+
|{10165, 10299, 12...|{Adak Island, Anc...|{Adak, Ted Steven...|{AK, AK, AK, FL, ...|
+--------------------+--------------------+--------------------+--------------------+

root
 |-- airport_id: struct (nullable = true)
 |    |-- 0: long (nullable = true)
 |    |-- 1: long (nullable = true)
 |    |-- 10: long (nullable = true)
 ```
 Para tener un formato tabular, debemos crear un ESQUEMA
 ```
from pyspark.sql.functions import *
from pyspark.sql.types import *

# CREANDO UN ESQUEMA PARA EL JSON
schema = StructType() \
             .add("airport_id", MapType(StringType(), IntegerType())) \
             .add("city", MapType(StringType(), StringType())) \
             .add("name", MapType(StringType(), StringType())) \
             .add("state", MapType(StringType(), StringType()))

# READ JSON USANDO EL ESQUEMA
df1 = spark.read.json(path= "dbfs:/FileStore/shared_uploads/tacnampt@gmail.com/airports-3.json", schema=schema)
df1.show(2)
df1.printSchema()
```
Ahora podemos observar que el databrame tiene formato key, value por cada campo.
```
+--------------------+--------------------+--------------------+--------------------+
|          airport_id|                city|                name|               state|
+--------------------+--------------------+--------------------+--------------------+
|{0 -> 10165, 1 ->...|{0 -> Adak Island...|{0 -> Adak, 1 -> ...|{0 -> AK, 1 -> AK...|
+--------------------+--------------------+--------------------+--------------------+

root
 |-- airport_id: map (nullable = true)
 |    |-- key: string
 |    |-- value: integer (valueContainsNull = true)
 |-- city: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
 |-- name: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
 |-- state: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
```

Con EXPLODE creamos cuatro dataframes en formato tabular
```
df1_a = df1.select(explode("airport_id").alias("id","airport_id")) 
df1_b = df1.select(explode("city").alias("id","city"))
df1_c = df1.select(explode("name").alias("id","name"))
df1_d = df1.select(explode("state").alias("id","state"))
```


Finalmente juntamos los cuatro dataframes haciendo JOIN
```
df_final = df1_a \
    .join(df1_b, df1_a.id == df1_b.id, "inner") \
    .join(df1_c, df1_a.id == df1_c.id, "inner") \
    .join(df1_d, df1_a.id == df1_d.id, "inner") \
    .select(df1_a.airport_id, df1_b.city, df1_c.name, df1_d.state)

df_final.show(5)
```
```
+----------+-----------+--------------------+-----+
|airport_id|       city|                name|state|
+----------+-----------+--------------------+-----+
|     10165|Adak Island|                Adak|   AK|
|     10299|  Anchorage|Ted Stevens Ancho...|   AK|
|     10304|      Aniak|       Aniak Airport|   AK|
|     10754|     Barrow|Wiley Post/Will R...|   AK|
|     10551|     Bethel|      Bethel Airport|   AK|
+----------+-----------+--------------------+-----+
```
