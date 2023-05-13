# Índice 

 - [Bicimad](#id1)

    - [Tema elegido](#id1.1)
    - [Ideas](#id1.2)
    - [Repositorios pyspark para ayuda](#id1.3)
    - [Datos que nos encontramos](#id1.4)

 - [Instrucciones del terminal](#id2)

    - [Github](#id2.1)
    - [Cluster](#id2.2)
    - [Hadoop](#id2.3)

# Bicimad <a name=id1> </a>

## Tema elegido <a name=id1.1> </a>

(Era algo así corregirlo quien quiera) 

Encontrar estaciones donde se necesiten más bicis (halla más demanda) y otras donde sobren (no tienen tanta demanda) para recolocar mejor la bicis por la ciudad. Tener en cuenta también las zonas. Ya que las demandas de las estaciones próximas entre sí estarán (lo normal) relacionadas. También ver si cómo cambia la demanda en los fines de semana, y a lo mejor proponer cambios los viernes por ejemplo de las bicis de un sitio a otro (luegares más lejanos de las zonas de trabajo sería lo normal). Así como observar cómo cambia la demanda durante el día. 

## Ideas <a name=id1.2> </a>

 - Sumar por días los plugs y unplugs de cada una de las estaciones. Así podemos ver las demandas de cada estación. Luego podemos compararlo por semanas o cualquier periodo de tiempo.

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

