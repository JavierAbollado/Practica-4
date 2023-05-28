
<img align="right" src="images/01-bicimad-escena-principal.gif" width="60%"/>

# Práctica 4 de spark

Integrantes:
* David Parro Plaza
* Francisco Javier Abollado
* Juan Álvarez San Romualdo

# Archivos <a name=id1.3> </a>

El repositorio cuenta con los siguientes archivos y carpetas

* carpeta .ipynb_checkpoints:  contiene los checkpoints de los ipynb con los que hemos estado trabajando
* carpeta datos: contiene los samples que utilizamos en local para realizar entrega.ipynb 
* carpeta images: contiene la portada 
* entrega.pdf: memoria completa
* entrega.ipynb: es el jupyter que contiene datos de la memoria y el proceso seguido, así como detalles de código a la hora de realizar el trabajo.
* main.py: es el script con todas las funciones de la memoria. Lo utilizamos para importar las funciones que vamos a utilizar a entrega.ipynb.
* maincluster.py : es el script simplificado de main.py que utilizamos para ejecutar en el cluster del wild 


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



