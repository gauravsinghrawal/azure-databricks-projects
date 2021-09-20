-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC #### Create a DB with external tables for raw data

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/storagegen2databricks/raw/circuits.csv",header=true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)
USING csv
OPTIONS (path "/mnt/storagegen2databricks/raw/races.csv",header=true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Create tables for JSON
-- MAGIC ##### Create constructors table(Single line JSON, Simple structure)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT, 
constructorRef STRING, 
name STRING, 
nationality STRING, 
url STRING
)
USING json
OPTIONS (path "/mnt/storagegen2databricks/raw/constructors.json",header=true);

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ##### Create drivers table(Single line JSON, Simple structure)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT, 
driverRef STRING, 
number INT, 
code STRING, 
name STRUCT<forename:STRING,surname:STRING>,
dob DATE,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/storagegen2databricks/raw/drivers.json",header=true);

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ##### Create results table(Single line JSON, Simple structure)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT, 
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points DOUBLE,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed DOUBLE,
statusId STRING
)
USING json
OPTIONS (path "/mnt/storagegen2databricks/raw/results.json",header=true);

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ##### Create pits table(Multi line JSON, Simple structure)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT, 
duration STRING,
lap INT, 
milliseconds INT, 
raceId INT,
stop INT,
time STRING
)
USING json
OPTIONS (path "/mnt/storagegen2databricks/raw/pit_stops.json",header=true,multiLine=true);

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
driverId INT, 
raceId INT,
lap INT, 
position INT, 
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/storagegen2databricks/raw/lap_times",header=true);

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
driverId INT, 
raceId INT,
lap INT, 
position INT, 
time STRING,
milliseconds INT
)
USING json
OPTIONS (path "/mnt/storagegen2databricks/raw/qualifying",header=true,multiLine=true);

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying;

-- COMMAND ----------


