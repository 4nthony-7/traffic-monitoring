# 🚦 Highway Speed Control — Spark Structured Streaming Demo

Démonstration d'**Apache Spark Structured Streaming** appliquée à un
contrôle automatisé de vitesse sur autoroute : deux radars séparés de 5 km
détectent les mêmes véhicules ; Spark joint les deux flux en temps réel,
calcule la vitesse moyenne et déclenche une alerte pour tout dépassement du
seuil légal.

---

## Stack technique

| Composant | Image / technologie | Rôle |
|-----------|-------------------|------|
| Kafka (KRaft) | `apache/kafka:latest` | Bus de messages |
| Spark master + 2 workers | `spark:python3` (Docker Official) | Moteur de stream processing |
| Producteurs radar | `python:3.11-slim` | Simulent les radars A et B |
| Dashboard | `python:3.11-slim` + Streamlit | Visualisation temps réel |

Toutes les images sont **open source**.

---

## Architecture

```
Radar A (km 0)                                   Radar B (km 0.5)
    │  {plate, radar_id:"A", ts_ms}                   │  {plate, radar_id:"B", ts_ms}
    │                                                  │
    └─────────────────────┐   ┌──────────────────────┘
                          ▼   ▼
                Kafka topic : radar_events
                            │
                    ┌───────▼────────┐
                    │   Spark job    │
                    │                │
                    │ filter A / B   │  ← même topic, deux flux logiques
                    │ withWatermark  │  ← tolérance aux événements tardifs
                    │ Stream JOIN    │  ← même plaque, B après A, < 30 min
                    │ speed = D / Δt │  ← 5 km / ((ts_b - ts_a) / 3 600 000)
                    │ window(30s)    │  ← statistiques par fenêtre glissante
                    └───────┬────────┘
                ┌──────────┼──────────┐
                ▼          ▼          ▼
          all_vehicles  violations  traffic_stats   (topics Kafka)
                └──────────┴──────────┘
                            │
                    Streamlit Dashboard :8501
```

### Concepts Spark Structured Streaming illustrés

|                      Concept                            |           Fonction                          |
|-------------------------------------------------        |---------------------------------------------|
| **lecture du flux kafka** :                             |             `readStream`                    |
| **traduction des évènements kafka**                     |                `EVENT_SCHEMA`               |
| **Gestion des retards** :                               |                      `withWatermark`        |
| **Gestion des bugs** :   Checkpoint                     |                      `CHECKPOINT_DIR`        |
| **Matchmaking sur les plaques** :                       |           `Stream–Stream JOIN`              |
| **Windowing** :  sink `traffic_stats`                   |             `window()` + `groupBy`          |
| **Ecriture des dataframes vers kafka**                  |                     `writeStream`           |
|**Micro-Batching**: satisfaire les sinks                 |             `trigger()`                     |

---

## Get Started

### Prérequis

- **Docker Desktop** ≥ 24 démarré et en cours d'exécution (icône verte dans la barre système)
- **Docker Compose** ≥ 2.20 (inclus dans Docker Desktop)
- **4 Go de RAM** minimum alloués à Docker Desktop
  (`Settings → Resources → Memory`)
- Ports disponibles : `7077`, `8080`, `8501`, `9094`

> **Windows** : ouvrez un terminal **PowerShell** ou **Git Bash** depuis le
> dossier `traffic-monitoring/`. Les commandes ci-dessous fonctionnent dans les deux.

---

### Étape 1 — Vérifier la structure des fichiers

Assurez-vous que votre dossier ressemble exactement à ceci avant de lancer quoi que ce soit :

```
traffic-monitoring/
├── docker-compose.yml
├── spark/
│   ├── Dockerfile
│   └── start-spark.sh          ← LF
├── producer/
│   ├── Dockerfile
│   └── producer.py
├── dashboard/
│   ├── Dockerfile
│   └── app.py
├── spark_jobs/
│   └── job_traffic.py
└── scripts/
    └── submit_job.sh
```

> ⚠️ **Windows uniquement** — `start-spark.sh` doit être encodé en **LF** (Unix),
> pas CRLF (Windows). Vérifiez dans VS Code : en bas à droite, l'indicateur doit
> afficher `LF`. Si vous voyez `CRLF`, cliquez dessus et sélectionnez `LF`.

---

### Étape 2 — Construire et démarrer les conteneurs

```bash
cd traffic-monitoring
docker compose up -d --build
```

La première fois, Docker télécharge les images de base (~1-2 Go) et construit
les images custom. Cela prend **3 à 5 minutes**.

Attendez que Kafka soit prêt (le healthcheck peut prendre 30 secondes) :

```bash
docker compose ps
# Tous les services doivent afficher "healthy" ou "running"
```

---

### Étape 3 — Soumettre le job Spark

```bash
# Linux / macOS / Git Bash
chmod +x scripts/submit_job.sh
./scripts/submit_job.sh

# PowerShell (Windows natif)
docker exec -d spark-master /opt/spark/bin/spark-submit 
  --master spark://spark-master:7077 
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" 
  --conf "spark.sql.shuffle.partitions=4" 
  --conf "spark.driver.host=spark-master" 
  /opt/spark_jobs/job_traffic.py
```

> Au premier lancement, Spark télécharge le connecteur Kafka (~30 s).
> Les lancements suivants sont quasi-instantanés (JAR mis en cache).

---

### Étape 4 — Ouvrir le dashboard

```
http://localhost:8501
```

Les premières données apparaissent **30 à 60 secondes** après le démarrage du
job, le temps que Spark accumule des paires appariées (Radar A + Radar B pour
la même plaque).

---

### Étape 5 — Inspecter le cluster Spark

```
http://localhost:8080
```

Vous verrez l'application `HighwaySpeedDetection` avec ses 3 streaming queries
actives : `all_vehicles`, `violations`, `traffic_stats`.

---

### Arrêter le projet

```bash
docker compose down          # arrête les conteneurs, conserve les volumes
docker compose down -v       # arrête et supprime les checkpoints Spark
```

---

## Commandes utiles

```bash
# Suivre les logs de tous les services
docker compose logs -f

# Suivre uniquement Kafka
docker compose logs -f kafka

# Suivre le master Spark
docker logs -f spark-master

# Voir les événements bruts (radars A et B)
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic radar_events

# Voir les violations en temps réel
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic violations

# Lister tous les topics Kafka
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

---

## Paramètres

| Paramètre | Fichier | Valeur par défaut |
|-----------|---------|-------------------|
| Distance A → B | `job_traffic.py` + `dashboard/app.py` | 5 km |
| Limite de vitesse | `job_traffic.py` + `dashboard/app.py` | 110 km/h |
| Tolérance retard (watermark) | `job_traffic.py` | 15 min |
| Fenêtre de join | `job_traffic.py` | 30 min |
| Fenêtre statistiques | `job_traffic.py` | 30 s |
| Intervalle d'émission radar | `docker-compose.yml` | 800 ms |

---

## Dépannage

**`path "spark" not found` au moment du build**
→ Le dossier `spark/` avec son `Dockerfile` et `start-spark.sh` est manquant.
Vérifiez la structure à l'Étape 1.

**Aucune donnée dans le dashboard après 2 minutes**
→ Vérifiez que le job tourne : `docker logs spark-master` doit mentionner
`HighwaySpeedDetection`. Le join stream–stream n'émet qu'une fois qu'une
paire A + B est appariée — attendez ~60 s après la soumission du job.

**Le job échoue à télécharger le JAR Kafka**
→ La première exécution requiert un accès internet depuis le conteneur.
Vérifiez que Docker Desktop a accès au réseau
(`Settings → Resources → Network`).

**Conflit sur le port 8080**
→ Un autre service (Jenkins, etc.) occupe ce port. Dans `docker-compose.yml`,
changez `"8080:8080"` en `"18080:8080"` et accédez à l'UI sur `:18080`.

**`start-spark.sh` : erreur `/bin/bash^M : bad interpreter`**
→ Le fichier a des fins de ligne Windows (CRLF). Ouvrez-le dans VS Code,
cliquez sur `CRLF` en bas à droite, sélectionnez `LF`, sauvegardez,
puis relancez `docker compose up -d --build`.


**Checkpoint:** : `CHECKPOINT_DIR`
Chaque fois qu'un micro-batch est terminé, Spark écrit deux choses vitales sur le disque :
Les Offsets : "J'ai lu Kafka jusqu'au message n°4500".
L'État (State) : "Je garde en mémoire que la plaque AB-123 est passée au Radar A mais j'attends encore le B".
