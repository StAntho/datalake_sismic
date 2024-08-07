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
> Analyse en découpant par mois -> non concluant car il n'y a que des données sur la même période
> Analyse par rapport au lieu
