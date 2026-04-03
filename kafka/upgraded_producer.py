import pandas as pd
import numpy as np
import random
import json
import time
from datetime import timedelta, datetime, date

from faker import Faker
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaProducer



##### CREATING TOPIC

admin = KafkaAdminClient(
    bootstrap_servers = 'localhost:9094',
    client_id = 'topic-creator'
)

topic = NewTopic(
    name = 'traffic_accidents',
    num_partitions = 4,
    replication_factor = 1
)

try:
    admin.create_topics([topic])
    print("Topic successfully created.")
except TopicAlreadyExistsError:
    print("Topic already exists. Continuing...")
finally:
    admin.close()

##### FAKER: GENERATING FAKE DATA

fake = Faker()

# ==============================
# LOAD DATA
# ==============================

df = pd.read_csv("data/sampled_accidents.csv")

if "Start_Time" in df.columns:
    df["Start_Time"] = pd.to_datetime(df["Start_Time"], errors="coerce")

# ==============================
# HELPERS
# ==============================

def normalize(series):
    return (series / series.sum()).to_dict()


def weighted_choice(weight_dict):
    keys = list(weight_dict.keys())
    weights = list(weight_dict.values())
    return random.choices(keys, weights=weights, k=1)[0]

# ==============================
# DISTRIBUTIONS
# ==============================

categorical_distributions = {}
for col in df.columns:
    if df[col].dtype == "str":
        vc = df[col].value_counts(dropna=True)
        if len(vc) > 0:
            categorical_distributions[col] = normalize(vc)

numeric_stats = {}
for col in df.select_dtypes(include=[np.number]).columns:
    numeric_stats[col] = {
        "mean": df[col].mean(),
        "std": df[col].std() if not np.isnan(df[col].std()) else 1
    }

null_probs = {col: df[col].isna().mean() for col in df.columns}

# Time distributions
if "Start_Time" in df.columns:
    df["hour"] = df["Start_Time"].dt.hour
    hour_weights = normalize(df["hour"].value_counts().sort_index())
else:
    hour_weights = None

# Severity | Weather
severity_given_weather = {}
if "Weather_Condition" in df.columns and "Severity" in df.columns:
    severity_given_weather = (
        df.groupby("Weather_Condition")["Severity"]
        .value_counts(normalize=True)
        .unstack(fill_value=0)
        .to_dict(orient="index")
    )

# ==============================
# GEO REALISM (CLUSTERING)
# ==============================

geo_clusters = {}

if "State" in df.columns and "Start_Lat" in df.columns:
    for state, group in df.groupby("State"):
        coords = group[["Start_Lat", "Start_Lng"]].dropna()
        if len(coords) > 0:
            geo_clusters[state] = coords.sample(min(1000, len(coords)))

state_weights = normalize(df["State"].value_counts()) if "State" in df.columns else {}

# ==============================
# WEATHER → MEASUREMENTS
# ==============================

weather_measurements = {}
measurement_cols = [
    "Visibility(mi)", "Humidity(%)", "Pressure(in)",
    "Wind_Speed(mph)", "Precipitation(in)"
]

if "Weather_Condition" in df.columns:
    for weather, group in df.groupby("Weather_Condition"):
        weather_measurements[weather] = {}
        for col in measurement_cols:
            if col in group.columns:
                weather_measurements[weather][col] = {
                    "mean": group[col].mean(),
                    "std": group[col].std() if not np.isnan(group[col].std()) else 1
                }

# ==============================
# ROAD FEATURE CORRELATIONS
# ==============================

road_features = [col for col in ["Traffic_Signal", "Crossing", "Junction"] if col in df.columns]

# P(feature | severity)
feature_given_severity = {}

if "Severity" in df.columns:
    for sev, group in df.groupby("Severity"):
        feature_given_severity[sev] = {}
        for col in road_features:
            feature_given_severity[sev][col] = group[col].mean()

# ==============================
# SAMPLERS
# ==============================

def sample_time():
    if hour_weights:
        hour = int(weighted_choice(hour_weights))
        base = fake.date_time_this_year()
        return base.replace(hour=hour)
    return fake.date_time_this_year()


def sample_geo():
    if not geo_clusters:
        return fake.latitude(), fake.longitude(), None

    state = weighted_choice(state_weights)
    cluster = geo_clusters.get(state)

    if cluster is not None and len(cluster) > 0:
        point = cluster.sample(1).iloc[0]
        lat = point["Start_Lat"] + np.random.normal(0, 0.01)
        lng = point["Start_Lng"] + np.random.normal(0, 0.01)
        return lat, lng, state

    return fake.latitude(), fake.longitude(), state


def sample_weather():
    return weighted_choice(categorical_distributions.get("Weather_Condition", {}))


def sample_severity(weather):
    if weather in severity_given_weather:
        return weighted_choice(severity_given_weather[weather])
    return weighted_choice(categorical_distributions.get("Severity", {}))


def sample_measurements(weather):
    values = {}

    if weather in weather_measurements:
        for col, stats in weather_measurements[weather].items():
            values[col] = float(np.random.normal(stats["mean"], stats["std"]))
    else:
        for col in measurement_cols:
            if col in numeric_stats:
                stats = numeric_stats[col]
                values[col] = float(np.random.normal(stats["mean"], stats["std"]))

    return values


def sample_road_features(severity):
    values = {}

    if severity in feature_given_severity:
        for col, prob in feature_given_severity[severity].items():
            values[col] = random.random() < prob
    else:
        for col in road_features:
            values[col] = random.random() < df[col].mean()

    return values


def sample_categorical(col):
    return weighted_choice(categorical_distributions.get(col, {})) if col in categorical_distributions else None


def sample_numeric(col):
    if col in numeric_stats:
        stats = numeric_stats[col]
        return float(np.random.normal(stats["mean"], stats["std"]))
    return None


def apply_null(col, value):
    return None if random.random() < null_probs.get(col, 0) else value

# Kafka producer needs the fields to be JSON serializable
def make_json_safe(obj):
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(x) for x in obj]
    if isinstance(obj, tuple):
        return [make_json_safe(x) for x in obj]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, (pd.Timestamp, datetime, date)):
        return obj.isoformat()
    if pd.isna(obj):
        return None
    return obj

# ==============================
# GENERATE ROW
# ==============================

def generate_row():
    row = {}

    start_time = sample_time()
    lat, lng, state = sample_geo()
    weather = sample_weather()
    severity = sample_severity(weather)
    measurements = sample_measurements(weather)
    road_vals = sample_road_features(severity)

    for col in df.columns:

        if col == "ID":
            value = fake.uuid4()

        elif col == "Start_Time":
            value = start_time

        elif col == "End_Time":
            value = start_time + pd.Timedelta(minutes=random.randint(5, 180))

        elif col == "Start_Lat":
            value = lat

        elif col == "Start_Lng":
            value = lng

        elif col == "State":
            value = state

        elif col == "Weather_Condition":
            value = weather

        elif col == "Severity":
            value = severity

        elif col == "Description":
            value = fake.sentence(nb_words=12)

        elif col in measurements:
            value = measurements[col]

        elif col in road_vals:
            value = road_vals[col]

        elif col in numeric_stats:
            value = sample_numeric(col)

        elif col in categorical_distributions:
            value = sample_categorical(col)

        elif df[col].dtype == "bool":
            value = random.random() < df[col].mean()

        else:
            value = None

        value = apply_null(col, value)
        row[col] = value

    return make_json_safe(row)



##### CREATE PRODUCER

def serializer(val):
    return json.dumps(val).encode("utf-8")

producer = KafkaProducer(
    bootstrap_servers = 'localhost:9094',
    linger_ms = 50,
    #key_serializer = lambda k: k.encode("utf-8") if k else None,
    value_serializer = serializer
)
print("Producer successfully created.")



### ACTUALLY GENERATING AND PRODUCING

try:
    count = 0
    start = time.time()
    print("Now generating events.")

    while True:

        # Create an event (a traffic accident)
        event = generate_row()

        # Send the event using producer
        producer.send(
            topic = "traffic_accidents",
            value = event
        )

        count += 1
        #time.sleep(.1)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()