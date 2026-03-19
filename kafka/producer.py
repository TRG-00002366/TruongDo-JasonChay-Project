from faker import Faker
import pandas as pd
import numpy as np
from datetime import timedelta, datetime, date
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaProducer
import json
import time
from numbers import Integral

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

admin.create_topics([topic])
admin.close()
print("Topic successfully created.")

##### FAKER: GENERATING FAKE DATA

df = pd.read_csv("data/sampled_accidents.csv")
fake = Faker()

# Null distributions in raw data
null_probs = df.isnull().mean().to_dict()

def apply_null(col, value):
    if np.random.rand() < null_probs.get(col, 0):
        return None
    return value

# Categorical distributions
severity_dist = df["Severity"].value_counts(normalize=True)
source_dist = df["Source"].value_counts(normalize=True)
weather_dist = df["Weather_Condition"].value_counts(normalize=True)
state_dist = df["State"].value_counts(normalize=True)
wind_dir_dist = df["Wind_Direction"].value_counts(normalize=True)
sun_dist = df["Sunrise_Sunset"].value_counts(normalize=True)

# Numerical mean and standard deviation 
distance_mean = df["Distance(mi)"].mean()
distance_std = df["Distance(mi)"].std()

temp_mean = df["Temperature(F)"].mean()
temp_std = df["Temperature(F)"].std()

humidity_mean = df["Humidity(%)"].mean()
humidity_std = df["Humidity(%)"].std()

pressure_mean = df["Pressure(in)"].mean()
pressure_std = df["Pressure(in)"].std()

visibility_mean = df["Visibility(mi)"].mean()
visibility_std = df["Visibility(mi)"].std()

wind_speed_mean = df["Wind_Speed(mph)"].mean()
wind_speed_std = df["Wind_Speed(mph)"].std()

# Boolean probabilities
bool_cols = ["Amenity","Bump","Crossing","Give_Way","Junction","No_Exit",
"Railway","Roundabout","Station","Stop","Traffic_Calming",
"Traffic_Signal","Turning_Loop"]

bool_probs = {c: df[c].mean() for c in bool_cols}

# Create city clusters
city_centers = (
    df.groupby(["City","State"])
    .agg(
        center_lat=("Start_Lat","mean"),
        center_lng=("Start_Lng","mean"),
        spread_lat=("Start_Lat","std"),
        spread_lng=("Start_Lng","std"),
        accident_count=("ID","count")
    )
    .reset_index()
)
# Keep cities with enough accidents to target major cities
accidents_for_major_city = 300
city_data = city_centers[city_centers["accident_count"] > accidents_for_major_city]

# 
def sample_city_cluster():

    row = city_data.sample(weights="accident_count").iloc[0]

    city = row.City
    state = row.State

    lat_center = row.center_lat
    lng_center = row.center_lng

    lat_spread = row.spread_lat
    lng_spread = row.spread_lng

    start_lat = np.random.normal(lat_center, lat_spread)
    start_lng = np.random.normal(lng_center, lng_spread)

    end_lat = start_lat + np.random.normal(0, lat_spread * 0.1)
    end_lng = start_lng + np.random.normal(0, lng_spread * 0.1)

    return city, state, start_lat, start_lng, end_lat, end_lng

# Compute Day/Night logic
def twilight_category(dt):

    hour = dt.hour

    if 6 <= hour < 18:
        return "Day","Day","Day","Day"
    elif 5 <= hour < 6 or 18 <= hour < 19:
        return "Night","Day","Day","Day"
    elif 4 <= hour < 5 or 19 <= hour < 20:
        return "Night","Night","Day","Day"
    else:
        return "Night","Night","Night","Night"

# Weather correlation generator
def generate_weather():

    condition = np.random.choice(
        weather_dist.index,
        p=weather_dist.values
    )

    temp = np.random.normal(temp_mean,temp_std)
    humidity = np.random.normal(humidity_mean,humidity_std)
    pressure = np.random.normal(pressure_mean,pressure_std)
    visibility = max(0.5,np.random.normal(visibility_mean,visibility_std))
    wind_speed = max(0,np.random.normal(wind_speed_mean,wind_speed_std))

    precipitation = 0

    if str(condition).startswith("Light"):
        precipitation = round(np.random.uniform(0.0,0.03))
        visibility -= np.random.uniform(1,5)

    elif "Rain" in str(condition):
        precipitation = round(np.random.uniform(0.5,5))
        visibility -= np.random.uniform(1,5)

    if "Cloud" in str(condition):
        humidity += np.random.uniform(5,10)
    
    if "Clear" in str(condition):
        humidity -= np.random.uniform(20,40)

    wind_chill = 35.74 + 0.6215*temp - 35.75*wind_speed**0.16 + 0.4275*temp*wind_speed**0.16

    return temp, wind_chill, humidity, pressure, visibility, wind_speed, precipitation, condition

# Datatypes need to be converted from np types to make them serializable for the producer
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


# Actual row generator
def generate_row():

    city, state, start_lat, start_lng, end_lat, end_lng = sample_city_cluster()

    start_time = fake.date_time_between("-8y","now")
    duration = np.random.randint(10,240)

    end_time = start_time + timedelta(minutes=duration)

    sunrise, civil, nautical, astro = twilight_category(start_time)

    temp, wind_chill, humidity, pressure, visibility, wind_speed, precipitation, condition = generate_weather()

    row = {
        "ID": apply_null("ID", f"A-{fake.random_number(digits=7)}"),

        "Source": apply_null("Source", np.random.choice(source_dist.index,p=source_dist.values)),

        "Severity": apply_null("Severity", np.random.choice(severity_dist.index,p=severity_dist.values)),

        "Start_Time": apply_null("Start_Time", start_time),
        "End_Time": apply_null("End_Time", end_time),

        "Start_Lat": apply_null("Start_Lat", round(start_lat,6)),
        "Start_Lng": apply_null("Start_Lng", round(start_lng,6)),

        "End_Lat": apply_null("End_Lat", round(end_lat,6)),
        "End_Lng": apply_null("End_Lng", round(end_lng,6)),

        "Distance(mi)": apply_null("Distance(mi)", abs(round(np.random.normal(distance_mean,distance_std),3))),

        "Description": apply_null("Description", fake.sentence()),

        "Street": apply_null("Street", fake.street_name()),
        "City": apply_null("City", city),
        "County": apply_null("County", fake.city()),
        "State": apply_null("State", state),
        "Zipcode": apply_null("Zipcode", fake.zipcode()),

        "Country": apply_null("Country", "US"),
        "Timezone": apply_null("Timezone", "US/Eastern"),

        "Airport_Code": apply_null("Airport_Code", fake.lexify(text="K???")),

        "Weather_Timestamp": apply_null("Weather_Timestamp", start_time - timedelta(minutes=np.random.randint(5,30))),

        "Temperature(F)": apply_null("Temperature(F)", round(temp,1)),
        "Wind_Chill(F)": apply_null("Wind_Chill(F)", wind_chill),
        "Humidity(%)": apply_null("Humidity(%)", round(humidity,1)),
        "Pressure(in)": apply_null("Pressure(in)", round(pressure,2)),
        "Visibility(mi)": apply_null("Visibility(mi)", round(visibility,1)),

        "Wind_Direction": apply_null("Wind_Direction", np.random.choice(wind_dir_dist.index,p=wind_dir_dist.values)),

        "Wind_Speed(mph)": apply_null("Wind_Speed(mph)", round(wind_speed,1)),

        "Precipitation(in)": apply_null("Precipitation(in)", precipitation),

        "Weather_Condition": apply_null("Weather_Condition", condition)
    }

    for col in bool_cols:
        val = np.random.rand() < bool_probs[col]
        row[col] = apply_null(col,val)

    row["Sunrise_Sunset"] = apply_null("Sunrise_Sunset", sunrise)
    row["Civil_Twilight"] = apply_null("Civil_Twilight", civil)
    row["Nautical_Twilight"] = apply_null("Nautical_Twilight", nautical)
    row["Astronomical_Twilight"] = apply_null("Astronomical_Twilight", astro)
    
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
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()