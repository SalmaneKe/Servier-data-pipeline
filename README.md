

# 📝 Réponses pour la section: Pour aller plus loin

## Gestion des données à grande échelle avec Polars sur GCP

### Résumé

- Même si la librairie Polars permet de gérer des volumes importants de données, ils existent d'autres solutions robustes sur GCP à savoir Dataflow (Apache Beam), Dataproc (PySpark), ou encore Bigquery (depuis dbt ou dataflow) qui sont des solutions managées ou encore serverless.
Cependant pour profiter pleinement des avantages que Polars présente, il faut provisionner les ressources adéquates pour garantir le passage à l'échelle et les calculs en temps réels.

La solution présentée consiste à traiter les 3 types de fichiers dans le même container. Afin de faire évoluer la solution pour qu'elle puisse prendre en compte un nombre important de fichiers, il convient d'abord de dédier à chaque traitement de fichier une tâche à part qui s'exécute indépendamment des autres taches dans un orchestrateur. Ceci permet de scale la tache lorsqu'il y a un nombre important de fichiers.
Par exemple on peut provisionner des Cloud Run (jobs) qui peuvent être notifiés via Cloud Pub/Sub et Cloud workflow pour s'éxécuter dés lors  de nouveaux fichiers sont déposés dans leurs buckets. Le Cloud Run (Job) les transforme et les stock dans des raw-datasets sur Bigquery. Ensuite un DAG Airflow, sur Composer, ou task Argo sur GKE peuvent être planifées selon des horaires ou déclenchés selon des évenements pour goldeniser la données et/ou calculer des cas d'usages métiers avec dbt/dataflow sur Bigquery.



### 1. Stockage de données

Pour des téra/péta-octets de données, GCP propose **Google Cloud Storage (GCS)** et **BigQuery** comme solutions de stockage principales.
.

- **Polars avec Parquet** : Polars offre un excellent support pour Parquet, qui est un format de stockage colonnaire efficace, idéal pour les charges de travail analytiques à grande échelle. On peut stocker des fichiers Parquet bruts dans GCS et les charger en mémoire avec Polars.

### 2. Infrastructure de calcul

#### Option 1 : Google Kubernetes Engine (GKE)
- **GKE** est un service Kubernetes géré qui peut être utilisé pour orchestrer des charges de travail conteneurisées. Polars peut être emballé dans un conteneur Docker et exécuté sur un cluster GKE.
- Avec GKE, On peut faire évoluer les charges de travail horizontalement en ajoutant plus de pods (conteneurs) ou verticalement en augmentant les ressources des nœuds (machines) individuels.
- **GCSFuse** ou **gcsfs** peuvent être utilisés dans vos conteneurs pour accéder directement aux données stockées dans GCS. Cette configuration permet de diffuser des données dans Polars à partir de GCS sans avoir à charger l'intégralité de l'ensemble de données dans le stockage local.

#### Option 2 : Google Cloud Dataflow
- **Dataflow** est le service de traitement de flux et de lot géré de GCP, basé sur Apache Beam.
- Polars peut être intégré dans les pipelines Dataflow via des **DoFns** (fonctions distribuées) personnalisés pour un traitement rapide en mémoire.
- Les fonctionnalités d'auto-scaling de Dataflow sont idéales pour gérer de grands ensembles de données, permettant d'augmenter automatiquement le nombre de travailleurs en fonction de la charge de travail.

#### Option 3 : Google Compute Engine (GCE)
- **GCE** fournit des machines virtuelles (VM) avec des configurations personnalisables de CPU et de mémoire.
- Polars peut fonctionner sur des instances à grande mémoire pour des opérations haute performance sur un seul nœud. Pour le traitement distribué, On peut combiner GCE avec des outils d'orchestration (par exemple, Dask ou Ray) pour répartir les calculs sur plusieurs machines virtuelles. (Managed Instance Groups)

### 3. Orchestration des workflows

#### Option 1 : Cloud Composer (Airflow)
- **Cloud Composer** est la version gérée par GCP d'Apache Airflow. Il vous permet de créer des graphes acycliques dirigés (DAG) pour orchestrer des tâches, telles que la lecture de données à partir de GCS, leur traitement avec Polars, et la sauvegarde des résultats dans GCS ou BigQuery.
- On peut planifier les charges de travail de traitement Polars avec Cloud Composer pour les exécuter en mode batch, déclenché par des événements comme le téléchargement de nouvelles données.

#### Option 2 : Cloud Functions et Cloud Run
- **Cloud Functions** : On peut écrire du code léger, déclenché par des événements, pour lancer des tâches Polars lorsque de nouvelles données arrivent dans GCS. Ceci est utile pour le traitement en temps réel ou quasi temps réel.
- **Cloud Run** : Déployer Polars dans un conteneur Docker sur Cloud Run, qui évolue automatiquement en fonction de la demande. Ceci est utile pour des services sans état et évolutifs (stateless) qui traitent des données à la demande.

---

Cette architecture permet à Polars de traiter efficacement des données à l'échelle tout en utilisant les services cloud natifs de GCP pour la gestion, l'orchestration, et l'évolutivité des workflows de données.
