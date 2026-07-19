# CS-675 code-starter — Docker workflow (macOS / Linux / WSL).
#
# Windows PowerShell users: use the parallel `make.ps1` in this directory
# with identical target names — e.g. `.\make.ps1 up` instead of `make up`.
# Native (uv-based) workflows: see README-mac.md or README-windows.md.

.PHONY: help up down restart logs shell hello test history clean \
        download-nyc-cab-data download-nyc-cab-zones-data download-nyc-bikes-data \
        download-shakespeare-data \
        analyze-shakespeare-data-use-case-a analyze-shakespeare-data-use-case-b \
        analyze-nyc-cab-data-use-case-a analyze-nyc-cab-data-use-case-b \
        analyze-nyc-cab-data-use-case-c analyze-nyc-cab-data-use-case-d \
        analyze-nyc-cab-data-use-case-e analyze-nyc-cab-data-use-case-f \
        analyze-nyc-bikes-data-use-case-a

# NYC TLC yellow-taxi Parquet (~48 MB / ~3 M rows) — the primary fact table.
NYC_CAB_URL  := https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
NYC_CAB_FILE := /home/jovyan/work/data/yellow_tripdata_2024-01.parquet

# NYC TLC taxi zone lookup CSV (~12 KB / 265 rows) — small dimension table.
NYC_CAB_ZONES_URL  := https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
NYC_CAB_ZONES_FILE := /home/jovyan/work/data/taxi_zone_lookup.csv

# JC Citi Bike monthly trips (~10 MB CSV / ~50 K rows) — large standalone CSV.
NYC_BIKES_URL  := https://s3.amazonaws.com/tripdata/JC-202401-citibike-tripdata.csv.zip
NYC_BIKES_ZIP  := /tmp/JC-202401-citibike-tripdata.csv.zip
NYC_BIKES_FILE := /home/jovyan/work/data/JC-202401-citibike-tripdata.csv

# Shakespeare's complete works from Project Gutenberg (~5.6 MB) — for word-count demos.
SHAKESPEARE_URL  := https://www.gutenberg.org/cache/epub/100/pg100.txt
SHAKESPEARE_FILE := /home/jovyan/work/data/shakespeare_complete_works.txt

help:
	@echo "CS-675 Code Starter — make targets"
	@echo ""
	@echo "Container lifecycle"
	@echo "  make up                                  Start containers (Jupyter :8888, Spark UI :4040, History :18080)"
	@echo "  make down                                Stop containers"
	@echo "  make restart                             Restart containers"
	@echo "  make logs                                Tail container logs"
	@echo "  make shell                               Open a bash shell inside the pyspark container"
	@echo "  make clean                               Stop and remove named volumes (drops event-log history too)"
	@echo ""
	@echo "Datasets (download once, reused by analyses)"
	@echo "  make download-nyc-cab-data               NYC TLC yellow-taxi Parquet   (~48 MB / ~3 M rows)"
	@echo "  make download-nyc-cab-zones-data         NYC TLC taxi zone lookup CSV  (~12 KB / 265 rows)"
	@echo "  make download-nyc-bikes-data             JC Citi Bike monthly CSV      (~10 MB / ~50 K rows)"
	@echo "  make download-shakespeare-data           Shakespeare complete works    (~5.6 MB plain text)"
	@echo ""
	@echo "Scripts 00–08 are numbered in rising order of complexity."
	@echo ""
	@echo "Analyses on Shakespeare text"
	@echo "  make analyze-shakespeare-data-use-case-a Word count, Spark        (01_word_count.py)          — MapReduce classic"
	@echo "  make analyze-shakespeare-data-use-case-b Word count, no Spark     (01_word_count_parallel.py) — faster here; Spark is overkill at this size"
	@echo ""
	@echo "Analyses on NYC cab data (same Parquet, different questions)"
	@echo "  make analyze-nyc-cab-data-use-case-a     Trip overview        (02_taxi_analysis.py)"
	@echo "  make analyze-nyc-cab-data-use-case-b     Tipping behavior     (03_taxi_tipping.py)"
	@echo "  make analyze-nyc-cab-data-use-case-c     Payment methods      (04_taxi_payments.py)"
	@echo "  make analyze-nyc-cab-data-use-case-d     Zones broadcast join (06_zones_analysis.py — also needs the zones CSV)"
	@echo "  make analyze-nyc-cab-data-use-case-e     Data preparation     (05_taxi_data_prep.py — Lecture 3)"
	@echo "  make analyze-nyc-cab-data-use-case-f     Classification (MLlib) (08_taxi_classification.py — Lecture 2b)"
	@echo ""
	@echo "Analyses on NYC bikes data"
	@echo "  make analyze-nyc-bikes-data-use-case-a   CSV vs Parquet       (07_citibike_analysis.py)"
	@echo ""
	@echo "Other"
	@echo "  make hello                               Smoke test (00_hello_spark.py)"
	@echo "  make test                                Run pytest inside the container"
	@echo "  make history                             Print the Spark History Server URL"

up:
	docker compose up -d
	@echo ""
	@echo "Jupyter Lab:    http://localhost:8888   (token: cs675)"
	@echo "Live Spark UI:  http://localhost:4040   (active while a SparkSession runs)"
	@echo "History Server: http://localhost:18080  (always up; shows past runs)"

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

shell:
	docker compose exec pyspark bash

hello:
	docker compose exec pyspark python /home/jovyan/work/00_hello_spark.py

# ----- Downloads --------------------------------------------------------------

download-nyc-cab-data:
	docker compose exec pyspark bash -c 'test -f $(NYC_CAB_FILE) && echo "Already downloaded: $(NYC_CAB_FILE)" || (echo "Downloading NYC cab data..." && curl -L --fail -o $(NYC_CAB_FILE) $(NYC_CAB_URL) && echo "Saved: $(NYC_CAB_FILE)")'

download-nyc-cab-zones-data:
	docker compose exec pyspark bash -c 'test -f $(NYC_CAB_ZONES_FILE) && echo "Already downloaded: $(NYC_CAB_ZONES_FILE)" || (echo "Downloading NYC cab zones data..." && curl -L --fail -o $(NYC_CAB_ZONES_FILE) $(NYC_CAB_ZONES_URL) && echo "Saved: $(NYC_CAB_ZONES_FILE)")'

download-nyc-bikes-data:
	docker compose exec pyspark bash -c 'test -f $(NYC_BIKES_FILE) && echo "Already downloaded: $(NYC_BIKES_FILE)" || (echo "Downloading NYC bikes data..." && curl -L --fail -o $(NYC_BIKES_ZIP) $(NYC_BIKES_URL) && unzip -p $(NYC_BIKES_ZIP) "JC-*.csv" > $(NYC_BIKES_FILE) && rm -f $(NYC_BIKES_ZIP) && echo "Saved: $(NYC_BIKES_FILE)")'

download-shakespeare-data:
	docker compose exec pyspark bash -c 'test -f $(SHAKESPEARE_FILE) && echo "Already downloaded: $(SHAKESPEARE_FILE)" || (echo "Downloading Shakespeare complete works..." && curl -L --fail -o $(SHAKESPEARE_FILE) $(SHAKESPEARE_URL) && echo "Saved: $(SHAKESPEARE_FILE)")'

# ----- Analyses ---------------------------------------------------------------

analyze-shakespeare-data-use-case-a:
	docker compose exec pyspark python /home/jovyan/work/01_word_count.py

analyze-shakespeare-data-use-case-b:
	docker compose exec pyspark python /home/jovyan/work/01_word_count_parallel.py

analyze-nyc-cab-data-use-case-a:
	docker compose exec pyspark python /home/jovyan/work/02_taxi_analysis.py

analyze-nyc-cab-data-use-case-b:
	docker compose exec pyspark python /home/jovyan/work/03_taxi_tipping.py

analyze-nyc-cab-data-use-case-c:
	docker compose exec pyspark python /home/jovyan/work/04_taxi_payments.py

analyze-nyc-cab-data-use-case-d:
	docker compose exec pyspark python /home/jovyan/work/06_zones_analysis.py

analyze-nyc-cab-data-use-case-e:
	docker compose exec pyspark python /home/jovyan/work/05_taxi_data_prep.py

analyze-nyc-cab-data-use-case-f:
	docker compose exec pyspark python /home/jovyan/work/08_taxi_classification.py

analyze-nyc-bikes-data-use-case-a:
	docker compose exec pyspark python /home/jovyan/work/07_citibike_analysis.py

# ----- Tests / utilities ------------------------------------------------------

test:
	docker compose exec pyspark python -m pip install -q pytest
	docker compose exec pyspark python -m pytest /home/jovyan/tests -v

history:
	@echo "Spark History Server: http://localhost:18080"
	@echo "(Refresh after a script finishes — completed apps appear automatically.)"

clean:
	docker compose down -v
