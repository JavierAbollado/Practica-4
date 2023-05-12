# Instrucciones generales del terminal


## Github

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



## Cluster

 - Activar el cluster:

```sshuttle -HNr usuario@wild.mat.ucm.es -x wild.mat.ucm.es```

 - Conetarnos al cluster:

```ssh usuario@wild.mat.ucm.es```

 - Añadir elementos a hadoop:

```scp -r nombre_fichero/ usuario@wild.mat.ucm.es:```


 - Número web para observar la interfaz gráfica:

```192.168.135.1:18081```


## Hadoop

```
hdfs dfs -put "archivo"
hdfs dfs -get "archivo"
hdfs dfs -rm "archivo"
hdfs dfs -ls

```

