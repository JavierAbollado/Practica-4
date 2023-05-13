# Índice 

 - [Bicimad](#id1)

    - [Tema elegido](#id1.1)
    - [Ideas](#id1.2)
    - [Repositorios pyspark para ayuda](#id1.3)
    - [Datos que nos encontramos](#id1.4)
    - [Preguntas](#id1.5)

 - [Instrucciones del terminal](#id2)

    - [Github](#id2.1)
    - [Cluster](#id2.2)
    - [Hadoop](#id2.3)

# Bicimad <a name=id1> </a>

## Tema elegido <a name=id1.1> </a>

(Era algo así corregirlo quien quiera) 

Encontrar estaciones donde se necesiten más bicis (halla más demanda) y otras donde sobren (no tienen tanta demanda) para recolocar mejor la bicis por la ciudad. Tener en cuenta también las zonas. Ya que las demandas de las estaciones próximas entre sí estarán (lo normal) relacionadas. También ver si cómo cambia la demanda en los fines de semana, y a lo mejor proponer cambios los viernes por ejemplo de las bicis de un sitio a otro (luegares más lejanos de las zonas de trabajo sería lo normal). Así como observar cómo cambia la demanda durante el día. 

## Ideas <a name=id1.2> </a>

### 1) Sumar por días los plugs y unplugs de cada una de las estaciones. Así podemos ver las demandas de cada estación. Luego podemos compararlo por semanas o cualquier periodo de tiempo.

```python 
# contar los plugs y unplugs
df1 = df.groupBy('idunplug_station').count()
df2 = df.groupBy('idplug_station').count()

# renombrar las columnas para unirlas
df1 = df1.withColumnRenamed('idunplug_station', 'id').withColumnRenamed('count', 'n_unplugs')
df2 = df2.withColumnRenamed('idplug_station', unplug_hourTime'id').withColumnRenamed('count', 'n_plugs')

# unirlas por el id de la estación
df3 = df1.join(df2, on='id')
```

### 2) En relación a tiempos de uso, se podría pensar (se verá en los datos) que los jovenes usan durante un mayor periodo de tiempo seguido las bicis, con respecto a los más mayores. Por lo que podemos medir eso:

```python
tiempo_total = df.groupBy('ageRange').sum('travel_time').orderBy('ageRange')
tiempo_medio = df.groupBy('ageRange').mean('travel_time').orderBy('ageRange')
```

aquí tenemos el tiempo total y tiempo medio, por edades. Recordar que el rango 0 son los datos desconocidos por lo que podemos hacer un ```drop(0)``` si no los queremos. Con esto podemos buscar desde de dónde salen más frecuentemente ciertos rangos de edades y a dónde van. Eso sería teniendo en cuenta la localización y las edades. A lo mejor agrupando en clusters por áreas (más complicado) y tener en cuenta dónde se necesitanm más. Un código podría ser:

 - Primero haríamos un preproceso de las localizaciones de la estaciones. En pseudocódigo sería algo como

```python
# Guardar puntos (x,y) con la posición geográfica en el mapa (lo podemos discretizar y eso para simplificar)
estaciones = df.id_station.discrete()
# le decimos en cuántos grupos queremos dividir la ciudad (k) y nos da los grupos hechos. 
grupos = keras.cluster(estaciones, k)     
# hacer de alguna manera una función lambda que nos pase el número de estación a su grupo. Y la guardamos en una nueva columna "groups"
df.groups = df.select('id_station').apply(lambda estacion : grupos(estacion))  
```

 - finalmente hacemos algo parecido a esto para tener por áreas el número de gente (por edades) que sale de cada zona, así como las bicis disponibles en dichas regiones:

```python
gente_por_zonas = df.groupBy('groups').groupBy('ageRange').count().orderBy('ageRange')
bicis_por_zonas = df.groupBy('groups').count()
```

## Repositorios pyspark para ayuda <a name=id1.3> </a>

 - https://github.com/krishnaik06/Pyspark-With-Python
 - https://github.com/SuperJohn/spark-and-python-for-big-data-with-pyspark
 - https://github.com/tirthajyoti/Spark-with-Python


## Datos que nos encontramos <a name=id1.4> </a>

```python
data0 = { 
    "_id" : { "$oid" : "5cf83b752f3843a016be4e2f" }, 
    "user_day_code" : "e4d55deb9ac172a8d8f5f0a32599815bd51b7c8760d67e42b11adf7c0829341b", 
    "idplug_base" : 21, 
    "user_type" : 1, 
    "idunplug_base" : 8, 
    "travel_time" : 219, 
    "idunplug_station" : 90, 
    "ageRange" : 0, 
    "idplug_station" : 66, 
    "unplug_hourTime" : { "$date" : "2019-06-01T00:00:00.000+0200" }, 
    "zip_code" : "" 
}
```

## Preguntas <a name=id1.5> </a>

### Diferencias entre Dataframes y RDD. ¿Son los dos distribuidos? -> Sí

In PySpark, both DataFrames and RDDs (Resilient Distributed Datasets) can be spread across multiple machines in a cluster.

DataFrames are built on top of RDDs and provide a higher-level API for working with structured data. Underneath the DataFrame API, the data is still distributed across multiple machines. DataFrames use the Catalyst optimizer to optimize and execute queries in a distributed manner.

When you perform transformations or actions on a DataFrame, the operations are executed in parallel across the cluster. The data is partitioned and processed in parallel on different machines, leveraging the distributed computing capabilities of Spark.

Similarly, RDDs in PySpark are distributed collections of data that are spread across multiple machines in the cluster. RDDs allow for fine-grained control over the distribution and parallel processing of data. RDDs can be created from various data sources and transformed using operations like map, filter, reduce, and more. These transformations and actions are executed in a distributed manner across the cluster.

So, both DataFrames and RDDs in PySpark can leverage the distributed computing capabilities of a cluster and distribute data across multiple machines for parallel processing.

### Qué hace exactamente 'toPandas()'? -> Costoso computacionalmente y pérdida de la capacidad distruida.

When you have a PySpark DataFrame, it is indeed distributed across multiple machines in the cluster. PySpark distributes the data and computation across the cluster to take advantage of parallel processing.

However, when you call the **toPandas()** function on a PySpark DataFrame, it converts the distributed DataFrame into a Pandas DataFrame, which is a local, in-memory data structure. The toPandas() function collects all the data from the distributed DataFrame and brings it back to the driver node as a Pandas DataFrame. At this point, the data is no longer distributed and resides entirely in memory on the driver node.

The resulting Pandas DataFrame obtained from **toPandas()** is a regular Pandas object, and any subsequent operations performed on it, including plotting and other Pandas-specific functions, are executed on a single machine. The data is no longer distributed across the cluster, and the distributed computing capabilities of PySpark are not utilized.

It's important to note that calling **toPandas()** can be an expensive operation because it requires transferring all the data from the distributed cluster to the driver node, which may cause memory issues if the DataFrame is large. It's generally recommended to perform data manipulations and computations on distributed PySpark DataFrames whenever possible to leverage the parallel processing capabilities of Spark.

# Instrucciones generales del terminal  <a name=id2> </a>


## Github  <a name=id2.1> </a>

 - Añadir clave ssh:

```ssh-add "directorio id_rsa"```

 - Quitar la clave ssh:

```ssh-add -D```

 - Actualizar github:

```git pull```


 - Pasos para hacer cambios:

```
git add "archivo"
git commit -m "mensaje"
git push
```



## Cluster  <a name=id2.2> </a>

 - Activar el cluster:

```sshuttle -HNr usuario@wild.mat.ucm.es -x wild.mat.ucm.es```

 - Conetarnos al cluster:

```ssh usuario@wild.mat.ucm.es```

 - Ejecutar spark en el cluster:

```spark-submit {archivo}```

 - Añadir elementos a hadoop:

```scp -r nombre_fichero/ usuario@wild.mat.ucm.es:```


 - Número web para observar la interfaz gráfica:

```192.168.135.1:18081```


## Hadoop  <a name=id2.3> </a>

```
hdfs dfs -put "archivo"
hdfs dfs -get "archivo"
hdfs dfs -rm "archivo"
hdfs dfs -ls

```

