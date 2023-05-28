
<img align="right" src="images/01-bicimad-escena-principal.gif" width="60%"/>

# Practica 4 de spark

Integrantes:
* David Parro Plaza
* Francisco Javier Abollado
* Juan Álvarez San Romualdo


# Índice 

 - [Bicimad](#id1)

    - [Tema elegido](#id1.1)
    - [Ideas](#id1.2)
    - [Archivos](#id1.3)
    - [Datos que nos encontramos](#id1.4)


# Bicimad Tema elegido <a name=id1> </a>


### La demanda del servicio de bicimad y las circunstancias que tienen un efecto sobre ella

* Cuántas salidas y entradas de bicicletas hay por barrios.
* Cómo afecta el día de la semana al uso de biciMad. 
* Salidas menos entradas para ver el uso porcentual por barrio.
* La máxima demanda de bicicletas en un barrio.
* Las conexiones mas usuales entre las distintas estaciones
* El crecimiento de la demanda del servicio a lo largo del tiempo. 
 

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



### 3) Demanda por zonas.

En relación a la separación por zonas anteior (clustering de las estaciones). Podemos observar individualmente la actividad de cada uno. Por ejemplo, guardar cuantas bicis salen de esa zona al día y además ver a dónde van. Para ello creamos dos nuevas columnas (para discretizar), donde guardamos los plugs y unplugs pero en vez de por estaciones, por grupos de estaciones:

 - 'idunplug_station_group' 
 - 'idplug_station_group'

luego para hacer el estudio completo agrupamos los datos por dias, es decir, guardamos como una tabla con los datos (salida desde la base hasta luegares de destino) de todos los lunes, martes,... Así podremos ver que días se separan más de la media. Es decir, si los sabados y domingos en un lado tiene una demanda mucho menor de lo habitual en una zona, y por el contrario en otra sube. Ahí es donde buscamos hacer los cambios. Para esto nuevo tendremos que crear un columna, 'day', con el dato: día de la semana (se puede sacar con un lambda de la parte de la fecha).

```python
# Agrupar por zonas: por días: los sitios a los que van,
# y por lo tanto, también tendremos la cantidad de movimiento de ese lugar.
estudio_por_dias_y_zonas = df.gruopBy('idunplug_station_group').groupBy('day').groupBy('idplug_station_group').count()

# Esquema de 'estudio_por_dias_y_zonas':
#
# id1 -> lunes  -> station 1 = [20,13,2,34,54,4,...] # estos son los datos de todos los lunes del mes / año ..
#                  station 2 = [10,23,4,34,74,4,...]
#                  ...
#        martes -> station 1 = [...]
#        ...
# id2 -> lunes -> ...
#
#

# Así para conseguir las listas con la media y desviación típica de dichas listas de valores hacemos:
media = estudio_por_dias_y_zonas.mean()
std   = estudio_por_dias_y_zonas.std()

# Luego filtramos por los que están muy por debajo de la media (o muy por encima). Para ello vemos si estan 
# fuera del 80% (o lo que sea) de una población normal de media y desviación típica las obtenidas.
por_encima = estudio_por_dias_y_zonas.filter((datos - media) / std > Z_alpha)
por_debajo = estudio_por_dias_y_zonas.filter((datos - media) / std < Z_alpha)
```

# Archivos <a name=id1.3> </a>

El repositorio cuenta con los siguientes archivos y carpetas


* carpeta .ipynb_checkpoints:  contiene los checkpoints de los ipynb con los que hemos estado trabajando
* carpeta datos: contiene los samples que utilizamos en local para realizar entrega.ipynb 
* carpeta images: contiene la portada 
* entrega.ipynb: es el jupyter que contiene la memoria y el proceso seguido a la hora de realizar el trabajo
* main.py: es el script que utilizamos para importar las funciones que vamos a utilizar a entrega.ipynb
* maincluster.py : es el srcipt simplificado de main.py que utilizamos para ejecutar en el cluster del wild 


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



