"""Paths and environment constants for the CS-675 code starter.

Resolves data paths against the container layout when inside Docker
(`/home/jovyan/work` exists) and against the local project tree otherwise.
"""
import os

IN_DOCKER = os.path.isdir("/home/jovyan/work")

WORK_DIR = "/home/jovyan/work" if IN_DOCKER else os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(WORK_DIR, "data")

TAXI_PARQUET    = os.path.join(DATA_DIR, "yellow_tripdata_2024-01.parquet")
ZONES_CSV       = os.path.join(DATA_DIR, "taxi_zone_lookup.csv")
CITIBIKE_CSV    = os.path.join(DATA_DIR, "JC-202401-citibike-tripdata.csv")
SHAKESPEARE_TXT = os.path.join(DATA_DIR, "shakespeare_complete_works.txt")

EVENT_LOG_DIR = "/spark-events"

LIVE_UI_URL = "http://localhost:4040"
HISTORY_URL = "http://localhost:18080"
