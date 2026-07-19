# Running the lab on Docker

By the end of this page you'll have a small Spark playground running on your laptop. It's two containers: one runs Jupyter Lab + PySpark (where your code lives) and the other runs the Spark History Server (where your past runs go). You'll use it for the rest of the semester, so it's worth getting comfortable with — but don't worry, it's a handful of commands, not an afternoon.

We're using Docker because it gives all of us the *same* Spark setup regardless of whether you're on a brand-new Mac, a Windows laptop, or a Linux desktop. You don't install Spark or Java; Docker handles all of that inside the container.

## Step 0 — Install Docker (one-time setup)

Find your operating system below. If Docker is already installed, jump to Step 1.

### macOS

1. Download **Docker Desktop** from <https://www.docker.com/products/docker-desktop/>.
2. Open the `.dmg` and drag Docker into Applications.
3. Launch Docker Desktop. The first launch can take a minute — wait for the whale icon in your menu bar to stop animating.

Apple Silicon (M1 / M2 / M3 / M4) and Intel Macs are both supported and Docker picks the right image variant for you automatically.

Verify it worked:

```
docker --version
docker compose version
```

Both commands should print a version. If you see "command not found", Docker Desktop probably isn't running — start it from Applications and try again.

### Windows

1. Make sure you're on Windows 10/11 with **WSL2** enabled. If you're not sure, open PowerShell as Administrator and run `wsl --install` — this enables it and installs Ubuntu by default. Reboot when it asks you to.
2. Download **Docker Desktop** from <https://www.docker.com/products/docker-desktop/>.
3. Run the installer. When asked, leave **"Use the WSL 2 based engine"** checked.
4. Launch Docker Desktop. Wait for the whale icon in your system tray to stop animating.

Verify it worked, in PowerShell:

```
docker --version
docker compose version
```

Both should print versions. If you see an error about the daemon not running, start Docker Desktop and wait for it to fully boot, then try again.

### Linux

You have two options. Docker Desktop is the easier path, Docker Engine is the lighter-weight one — either works for this course.

- **Docker Desktop**: download from <https://www.docker.com/products/docker-desktop/>, follow the per-distro install steps. Same flow as macOS/Windows.
- **Docker Engine** (recommended for native Linux): follow <https://docs.docker.com/engine/install/> for your distro. After installation, add your user to the `docker` group so you don't need `sudo`: `sudo usermod -aG docker $USER` and then log out and back in.

Verify:

```
docker --version
docker compose version
```

If `docker` says "permission denied", you skipped the `usermod -aG docker` step (or didn't log out and back in).

### Which shell will I be running commands in?

For the rest of this page we'll use `make <target>` for brevity. If you're on Windows PowerShell, just substitute `.\make.ps1 <target>` instead — the targets are identical.

| You're on | Use |
|---|---|
| macOS Terminal, Linux terminal, WSL2 shell | `make <target>` |
| Windows PowerShell directly | `.\make.ps1 <target>` |

If you're on Windows but happy in WSL2, use the Linux/Mac form — it's a bit nicer than PowerShell for things like piping.

## Step 1 — Start the lab

From inside the `code-starter/` directory:

```
make up
```

The first time you run this, Docker will pull about **1.5 GB** of image. That's a one-time cost — subsequent starts take a few seconds.

When it finishes, you'll have three web pages available:

- **Jupyter Lab** at <http://localhost:8888> — open this in a browser; the token is `cs675`.
- **Live Spark UI** at <http://localhost:4040> — only alive while a script is running. Useful for watching a job in progress.
- **Spark History Server** at <http://localhost:18080> — always up. Every script you run shows up here after it finishes. This is where you'll go to compare runs and explain "why was this slow?".

> The first two are the obvious ones. The History Server matters more than it sounds — for the rest of the course, when we discuss query planning and optimization, you'll go here to see what actually happened on real runs.

## Step 2 — Confirm everything works

Run the smoke test:

```
make hello
```

You'll see something like:

```
PySpark version: 4.1.1
Default parallelism: 6
Spark master:        local[*]
Live Spark UI:       http://localhost:4040  (during this run)
History Server:      http://localhost:18080  (after this run finishes)
+---+---------+
|  x|x_squared|
+---+---------+
|  0|        0|
|  1|        1|
...
Smoke test passed.
```

If you see `Smoke test passed.` you're done with setup. If anything else happens, skip to *When things go wrong* below.

## Step 3 — Run your first analysis

Now let's do something real. The recurring dataset for this course is **NYC TLC yellow-taxi trips** — a real, public, somewhat messy dataset of every yellow-cab ride in NYC. We'll work with January 2024 (about 3 million rides).

Download it:

```
make download-nyc-cab-data
```

This pulls about 48 MB of Parquet from the NYC TLC's CDN. Should take a few seconds.

Then run the headline analysis:

```
make analyze-nyc-cab-data-use-case-a
```

You'll see top pickup hours, average fare by passenger count, and the top 10 longest trips. The "longest trip" is going to look ridiculous (the script reports rides of 300,000+ miles, which is obviously a sensor glitch) — that's not a bug, that's *real* data. We'll talk about cleaning these in the data-prep lecture.

After it finishes, open <http://localhost:18080> in your browser. Your run should show up there within a couple seconds. Click into it to explore the DAG, the SQL plan, and per-stage timings — this is the view we'll come back to all semester.

## Step 4 — Explore the other analyses

The starter ships four datasets and eight analyses. The pattern: `download-*` to fetch a dataset, `analyze-*` to run a script over it. The `analyze-nyc-cab-data-use-case-{a,b,c,e,f}` targets all read the *same* taxi Parquet but ask different questions — that's how you'll usually work in practice.

### Datasets

| Target | What it is |
|---|---|
| `make download-nyc-cab-data` | NYC TLC yellow-taxi Parquet (~48 MB, ~3 M rows). The primary fact table. |
| `make download-nyc-cab-zones-data` | NYC TLC zone lookup CSV (~12 KB, 265 rows). The dimension table for `PULocationID` / `DOLocationID` in the Parquet. |
| `make download-nyc-bikes-data` | JC Citi Bike monthly CSV (~10 MB, ~50 K rows). A standalone dataset, no relationship to the taxi data. |
| `make download-shakespeare-data` | Shakespeare's complete works (~5.6 MB plain text from Project Gutenberg). The text corpus for the classic word-count example. |

### Analyses

Scripts are numbered `00` → `08` in **rising order of complexity**. `00` is just "make a tiny DataFrame"; by `08` you're training a logistic-regression classifier. Each script layers one or two new PySpark concepts onto the previous. If this is your first time with PySpark, run them in order; if you're comfortable, jump straight to whichever interests you.

| Target | Script | What it does |
|---|---|---|
| `make analyze-shakespeare-data-use-case-a` | `01_word_count.py` | **Word count** — the classic MapReduce example. Reads text → tokenizes → counts. Anchors the MapReduce → Spark bridge in Lecture 4. |
| `make analyze-nyc-cab-data-use-case-a` | `02_taxi_analysis.py` | **Trip overview** — top pickup hours, fare-vs-passenger-count, longest trips. |
| `make analyze-nyc-cab-data-use-case-b` | `03_taxi_tipping.py` | **Tipping behavior** — tip % by payment type, by hour, and the distribution. |
| `make analyze-nyc-cab-data-use-case-c` | `04_taxi_payments.py` | **Payment methods** — credit vs cash share, revenue per method, refund rate. |
| `make analyze-nyc-cab-data-use-case-e` | `05_taxi_data_prep.py` | **Data preparation** (Lecture 3) — missing-value inspection, median imputation, IQR outlier detection, z-score normalization, equal-frequency binning, one-hot encoding. |
| `make analyze-nyc-cab-data-use-case-d` | `06_zones_analysis.py` | **Zones join** — broadcasts the small lookup CSV against the big Parquet, then aggregates by borough. *Needs both* `download-nyc-cab-data` *and* `download-nyc-cab-zones-data`. |
| `make analyze-nyc-bikes-data-use-case-a` | `07_citibike_analysis.py` | **CSV → Parquet** — reads the bikes CSV with and without a declared schema, converts to Parquet, compares sizes and read times. |
| `make analyze-nyc-cab-data-use-case-f` | `08_taxi_classification.py` | **Classification with MLlib** (Lecture 2b) — predict whether a credit-card trip got a tip. VectorAssembler + LogisticRegression + AUC evaluation. |

Each script finishes in a few seconds. Try them in any order; they're independent. Open <http://localhost:18080> after each to see the run land there.

### How the scripts are organized (peek inside)

Each script lives in `work/` as a tiny, focused Python file. They share three helpers so the analyses themselves stay short:

- `work/constants.py` — data paths and URLs.
- `work/spark_helper.py` — a `get_spark(name)` that builds a SparkSession (with event logging into the History Server) and a `require_files(...)` that errors usefully if a dataset isn't downloaded yet.

If you want to write your own analysis, copy any of `02_…` through `04_…` (the simpler taxi scripts) — they're around 50 lines each, and the pattern is the same every time: import helpers, require files, do the analysis, print the UI URLs.

## All targets at a glance

```
make help
```

…or here's the quick reference:

| Target | What it does |
|---|---|
| `make up` | Start both containers |
| `make down` | Stop both containers (your files in `work/` stay on your laptop) |
| `make restart` | Stop and start again |
| `make logs` | Tail container logs |
| `make shell` | Open a bash shell inside the pyspark container |
| `make hello` | Run the smoke test (`00_hello_spark.py`) |
| `make download-nyc-cab-data` | Download the NYC taxi Parquet |
| `make download-nyc-cab-zones-data` | Download the NYC taxi zone lookup CSV |
| `make download-nyc-bikes-data` | Download the JC Citi Bike CSV |
| `make download-shakespeare-data` | Download Shakespeare's complete works |
| `make analyze-shakespeare-data-use-case-a` | Run the word-count analysis (`01_word_count.py`) |
| `make analyze-nyc-cab-data-use-case-a` | Run the trip-overview analysis (`02_taxi_analysis.py`) |
| `make analyze-nyc-cab-data-use-case-b` | Run the tipping analysis (`03_taxi_tipping.py`) |
| `make analyze-nyc-cab-data-use-case-c` | Run the payments analysis (`04_taxi_payments.py`) |
| `make analyze-nyc-cab-data-use-case-d` | Run the zones broadcast-join analysis (`06_zones_analysis.py`) |
| `make analyze-nyc-cab-data-use-case-e` | Run the data-prep walkthrough (`05_taxi_data_prep.py`) |
| `make analyze-nyc-cab-data-use-case-f` | Run the MLlib classification (`08_taxi_classification.py`) |
| `make analyze-nyc-bikes-data-use-case-a` | Run the Citi Bike CSV/Parquet comparison (`07_citibike_analysis.py`) |
| `make test` | Run the pytest suite |
| `make history` | Print the History Server URL |
| `make clean` | Stop containers and remove named volumes (wipes event-log history) |

## When things go wrong

A small list of the most common stumbles. Don't panic — fix the obvious one and re-run; nothing here is destructive.

- **"docker: command not found"** — Docker Desktop probably isn't running. Open it from your Applications / Start Menu, wait for the whale icon to stop animating, then try again.
- **"port already in use" on 8888, 4040, or 18080** — something else on your laptop is using that port. Open `docker-compose.yml` and change the host-side port (e.g. `"18888:8888"`), then `make up`.
- **`make hello` fails with a Python or PySpark error** — try `make down && make up && make hello`. Containers occasionally need a clean restart.
- **An analysis fails saying "missing /home/jovyan/work/data/…"** — you skipped the download step. The error message tells you which `make download-*` to run.
- **The History Server shows no runs** — runs appear a few seconds after a script *finishes*. If the script is still running, that's normal. If a finished run never shows up, check `docker compose logs spark-history`.
- **`.\make.ps1` won't run on Windows** — PowerShell is blocking unsigned scripts. Once per session: `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`.
- **Everything is mysteriously slow or weird** — `make clean && make up` is the nuclear option. Resets to a clean state, including the History Server event log. Re-download datasets if you'd already pulled them.

## Stopping the lab

When you're done for the day:

```
make down
```

Your files in `work/` stay on your laptop. Past runs in the History Server are preserved in a named volume — next time you `make up`, they're still there.

If you want a clean slate (e.g. to free a few GB of disk):

```
make clean
```

That stops the containers *and* removes the named volume (so the History Server forgets your past runs). You can always re-download the datasets and start fresh.

## Where to next

- **Native install instead** — if you'd rather skip Docker, see [README-mac.md](README-mac.md) or [README-windows.md](README-windows.md). You'll lose the History Server (the live Spark UI on `:4040` still works) but everything else is the same.
- **Multi-arch note** — the image is multi-arch (`linux/amd64` + `linux/arm64`), so Apple Silicon Macs run it natively, no Rosetta. CPU performance is near-native everywhere; file I/O across the host ↔ container bind mount has a small overhead on Intel Mac and Windows.
- **The dataset folder** — see [`work/data/README.md`](work/data/README.md) for the full list of files we use, plus manual download commands for when you can't use Make.

If you get stuck on something not covered above, post in the course channel — odds are someone else hit the same thing.
