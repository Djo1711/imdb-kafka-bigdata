# IMDB Data Analysis & Kafka Stream Processing

Big Data group project, ING5 at ECE Paris.

## Project Overview

This Big Data project consists of two main parts:

1. **IMDB Data Analysis**: Comprehensive analysis of IMDB movie datasets answering 14 specific questions
2. **Kafka Stream Processing**: Real-time monitoring of Wikipedia events using Apache Kafka

## Table of Contents
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Part 1: IMDB Analysis](#part-1-imdb-data-analysis)
- [Part 2: Kafka Streaming](#part-2-kafka-stream-processing)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)

## Prerequisites

### Software Requirements
- **Python 3.8+**
- **Docker and Docker Compose** (for Kafka)
- **Jupyter Notebook**
- **At least 8 GB RAM**
- **At least 10 GB free disk space**

### Check Your Setup
```bash
python3 --version
docker --version
docker-compose --version
jupyter --version
```

## Installation

### 1. Clone/Download Project
```bash
git clone https://github.com/Djo1711/imdb-kafka-bigdata.git
cd imdb-kafka-bigdata
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Start Kafka (for Part 2)
```bash
docker-compose up -d
```

Verify Kafka is running:
```bash
docker-compose ps
```

You should see both `zookeeper` and `kafka` containers running.

## Part 1: IMDB Data Analysis

### Running the Analysis

1. **Launch Jupyter Notebook**:
```bash
jupyter notebook imdb_analysis.ipynb
```

2. **Execute the notebook**:
   - Click "Cell" → "Run All" in the menu
   - Or execute cells one by one with Shift+Enter

3. **What happens**:
   - Downloads ~5 GB of IMDB data (10-20 minutes)
   - Loads data into pandas DataFrames
   - Answers all 14 questions
   - Displays results in markdown cells

### Questions Answered

1. ✓ Load data from IMDB (automated)
2. Total number of people in dataset
3. Earliest year of birth
4. Years since that birth
5-6. Verify birth date validity with reasoning
7. Most recent birth date
8. Percentage without birth date
9. Length of longest "short" after 1900
10. Length of shortest "movie" after 1900
11. Complete list of genres
12. Highest rated comedy movie
13. Director of that movie
14. Alternate titles for that movie

### Data Files Downloaded

The notebook automatically downloads these files to `data/`:
- `name.basics.tsv.gz` - People data
- `title.basics.tsv.gz` - Title data
- `title.crew.tsv.gz` - Crew information
- `title.ratings.tsv.gz` - Ratings
- `title.akas.tsv.gz` - Alternate titles
- `title.principals.tsv.gz` - Principal cast/crew
- `title.episode.tsv.gz` - TV episode data

## Part 2: Kafka Stream Processing

### Architecture

```
Wikimedia EventStreams → Kafka Producer → Kafka Broker → Kafka Consumer → Metrics/Alerts
```

### Monitored Entities

We track 5 entities with active Wikipedia pages:

1. **The Shawshank Redemption** - Classic film
2. **Christopher Nolan** - Active director
3. **Leonardo DiCaprio** - High-profile actor
4. **Marvel Cinematic Universe** - Popular franchise
5. **Academy Awards** - Annual event

### Metrics Collected

For each entity:
- Total number of edits
- Unique editors count
- Bytes added/removed
- Edit type distribution
- Bot vs human edits
- Last update timestamp

### Alert System

**Trigger**: Edits changing more than 5,000 bytes
**Action**: Write to `stream_data/alerts.json`
**Purpose**: Detect major content changes

### Running the Stream Processing

**Terminal 1: Start Kafka Producer**
```bash
python kafka_producer.py
```

This connects to Wikimedia EventStreams and publishes events to Kafka.

**Terminal 2: Start Kafka Consumer**
```bash
python kafka_consumer.py
```

This consumes events from Kafka and calculates metrics.

**Let both run for at least 15-30 minutes** to collect meaningful data.

### Viewing Results

**Real-time**: Watch the console output in both terminals

**Statistics**:
```bash
python kafka_consumer.py stats
```

**Files**:
- `stream_data/metrics.json` - Collected metrics
- `stream_data/alerts.json` - Triggered alerts

### Stopping the System

1. Stop producer: `Ctrl+C` in Terminal 1
2. Stop consumer: `Ctrl+C` in Terminal 2
3. Stop Kafka:
```bash
docker-compose down
```

To completely remove Kafka data:
```bash
docker-compose down -v
```

## Project Structure

```
imdb-big-data-project/
├── README.md                   # This file
├── requirements.txt            # Python dependencies
├── docker-compose.yml          # Kafka configuration
├── imdb_analysis.ipynb         # Main analysis notebook
├── kafka_producer.py           # Wikimedia to Kafka
├── kafka_consumer.py           # Kafka to metrics
├── ANSWERS.md                  # Template for answers
├── data/                       # IMDB data (created automatically)
│   ├── name.basics.tsv.gz
│   ├── title.basics.tsv.gz
│   └── ...
└── stream_data/                # Streaming results (created automatically)
    ├── metrics.json
    └── alerts.json
```

## Troubleshooting

### IMDB Analysis Issues

**Problem**: "Memory Error"
- **Solution**: Close other applications, ensure at least 8 GB RAM available

**Problem**: Download timeout
- **Solution**: Check internet connection, re-run the notebook (already downloaded files won't re-download)

**Problem**: "File not found"
- **Solution**: Ensure `data/` directory exists, check file permissions

### Kafka Issues

**Problem**: "Connection refused" to Kafka
```bash
# Check if Kafka is running
docker-compose ps

# Check logs
docker-compose logs kafka

# Restart Kafka
docker-compose restart
```

**Problem**: "No events being published"
- **Solution**: Wikimedia stream may be slow, wait 5-10 minutes

**Problem**: "Topic not found"
- **Solution**: Kafka auto-creates topics, but you can manually create:
```bash
docker exec -it kafka kafka-topics --create --topic wikimedia-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

**Problem**: High CPU usage
- **Solution**: Normal for Kafka, but you can limit Docker resources in Docker Desktop settings

### General Issues

**Problem**: "Module not found"
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

**Problem**: Jupyter won't start
```bash
pip install --upgrade jupyter notebook
```

## Performance Tips

### For IMDB Analysis
- Use a fast SSD for data storage
- Close unnecessary applications
- Consider using `low_memory=False` is already set in code

### For Kafka Streaming
- Monitor disk space (Kafka logs grow over time)
- Adjust `SAVE_INTERVAL` in consumer if needed
- Use `docker stats` to monitor resource usage

## Expected Runtime

| Task | Duration |
|------|----------|
| IMDB data download | 10-20 min |
| IMDB analysis execution | 30-60 min |
| Kafka setup | 2-5 min |
| Stream collection (minimum) | 15-30 min |
| Documentation | 30-60 min |
| **Total** | **2-4 hours** |

## References

- IMDB Datasets: https://datasets.imdbws.com/
- Wikimedia EventStreams: https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams
- Apache Kafka: https://kafka.apache.org/
- Pandas Documentation: https://pandas.pydata.org/

## License

This project is for academic use only.
