# Ayudas

## Acceder a los archivos grandes de Luis

```
hdfs dfs -ls /public/bicimad
```

para hacer copias de archivos dentro del hdfs accedemos a la dirección completa ```hdfs://master:9000/public/bicimad```.

## Copiar los archivos del cluster al ordenador local

Desde la consola del cluster, copiamos los datos de "carpeta" en local en la carpeta común "Descargas" del ordenador.

 - IP : 147.96.133.20 (este es un ejemplo)
 - nombre : alumno (esto es en el caso de los ordenadores de la uni, en casa será el nombre local del ordenador)

```
scp -r "carpeta" alumno@147.96.133.20:/home/alumno/Descargas/"carpeta"
```

te pedirá una constraseña por permisos, en la uni es la típica ```aulamates``` y en local lo que uno tenga personalmente.


# Cómo lidiar con las columnas de los distintos .json

Nos dan problemas con las columnas de los DataFrames, por tanto primero veamos qué columnas tenemos. Para ello creamos un pequeño script ```show_columns.py``` con el siguiente código:

```python
if __name__ == "__main__":

    import sys

    if len(sys.argv) > 2:
        n = len(sys.argv)
        geo_path = sys.argv[1]
        paths = [sys.argv[i] for i in range(2,n)]
    else:
        print("main.py <path to geo csv> <path to dataframe>")
        sys.exit(0)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # load & join all the json files
    ss = ""
    for path in paths:
        df = spark.read.json(path)
        ss += str(df.columns) + "\n"
    f = open("columns.txt", "w")
    f.write(ss)
    f.close()
```

y porterminal le pasamos todos los archivos del cluters:

```
python3 show_columns.py bases_bicimad.csv bicimad/201707_movements.json bicimad/201710_movements.json bicimad/201711_movements.json bicimad/201712_movements.json bicimad/201801_movements.json bicimad/201802_movements.json bicimad/201803_movements.json bicimad/201804_movements.json bicimad/201805_movements.json bicimad/201806_movements.json
```

dándonos como resultado lo siguiente:

```
['_corrupt_record', '_id', 'ageRange', 'idplug_base', 'idplug_station', 'idunplug_base', 'idunplug_station', 'track', 'travel_time', 'unplug_hourTime', 'user_day_code', 'user_type', 'zip_code']
['_corrupt_record', '_id', 'ageRange', 'idplug_base', 'idplug_station', 'idunplug_base', 'idunplug_station', 'track', 'travel_time', 'unplug_hourTime', 'user_day_code', 'user_type', 'zip_code']
['_id', 'ageRange', 'idplug_base', 'idplug_station', 'idunplug_base', 'idunplug_station', 'travel_time', 'unplug_hourTime', 'user_day_code', 'user_type', 'zip_code']
['_id', 'ageRange', 'idplug_base', 'idplug_station', 'idunplug_base', 'idunplug_station', 'travel_time', 'unplug_hourTime', 'user_day_code', 'user_type', 'zip_code']
['_id', 'ageRange', 'idplug_base', 'idplug_station', 'idunplug_base', 'idunplug_station', 'travel_time', 'unplug_hourTime', 'user_day_code', 'user_type', 'zip_code']
['_id', 'ageRange', 'idplug_base', 'idplug_station', 'idunplug_base', 'idunplug_station', 'travel_time', 'unplug_hourTime', 'user_day_code', 'user_type', 'zip_code']
['_id', 'ageRange', 'idplug_base', 'idplug_station', 'idunplug_base', 'idunplug_station', 'travel_time', 'unplug_hourTime', 'user_day_code', 'user_type', 'zip_code']
['_id', 'ageRange', 'idplug_base', 'idplug_station', 'idunplug_base', 'idunplug_station', 'travel_time', 'unplug_hourTime', 'user_day_code', 'user_type', 'zip_code']
['_corrupt_record', '_id', 'ageRange', 'idplug_base', 'idplug_station', 'idunplug_base', 'idunplug_station', 'track', 'travel_time', 'unplug_hourTime', 'user_day_code', 'user_type', 'zip_code']
['_corrupt_record', '_id', 'ageRange', 'idplug_base', 'idplug_station', 'idunplug_base', 'idunplug_station', 'track', 'travel_time', 'unplug_hourTime', 'user_day_code', 'user_type', 'zip_code']
```

por lo que para las ejecuciones eliminaremos las columnas "_corrupt_record" y "track" que aparecen de forma extra en algunas ocasiones y no utilizamos para nuestro estudio.
