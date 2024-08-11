# DataLake - Données Sismiques

> Suivi des données en direct d'un capteur fictif en python

## Historique de la mise en place

Load docker images

```bash
  docker compose up -d
```

Déplacement des fichiers en local sur un contener

```bash
  docker cp ../path_local_file.csv nom_du_contener:path_ou_stocker_file
```

Accéder au bash du contener

```bash
  docker exec -it nom_contener /bin/bash
```

Déplacer le fichier sur un hdfs (sur le bash contener-hadoop)

```bash
  hadoop fs -put file.csv /path/dans/hdfs
```

Déplacer le fichier sur un hdfs (sur le bash contener-hadoop)

```bash
  hadoop fs -put file.csv /path/dans/hdfs
```

Lire le fichier sur un hdfs (sur le bash contener-hadoop)

```bash
  hadoop fs -cat /path/dans/hdfs/file.csv
```

Création du fichier python explore_csv.py
Execution du code dans le bash contener-spark
Ajout de la suppression des doublons

> Analyse pour trouver des corrélations
> Calcul de corrélation entre la tension des plaques et la magnitude
> Analyse en découpant par mois -> non concluant car il n'y a que des données sur la même période
> Analyse par rapport au lieu
> Analyse par rapport à la tension des plaques

Dans le df_ville, ajout de colonnes temporelles pour l'analyse de séquences temporelle

Filtration sur les éléments précurseurs des petits et gros seismes

Agregation des données sur 1 journée et par heure

Ajout du producer kafka (changement de mode de lecture pandas -> spark)

Branchement du spark_streaming au producer kafka (sur le bash spark-master)

```bash
  /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 app/spark_streaming.py
```

Lancement de superset(sur le bash superset)

```bash
  superset db upgrade && superset init && superset run -h 0.0.0.0
```

#### Problème rencontrer

> erreur de port pour faire tourner superset
