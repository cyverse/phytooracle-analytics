import json
import re

# Define column names in snake_case
COLUMN_NAMES = [
    "year", "day_of_year", "station_number", "air_temp_max", "air_temp_min",
    "air_temp_mean", "rh_max", "rh_min", "rh_mean", "vpd_mean",
    "solar_radiation_total", "precipitation_total", "soil_temp_4in_max",
    "soil_temp_4in_min", "soil_temp_4in_mean", "soil_temp_20in_max",
    "soil_temp_20in_min", "soil_temp_20in_mean", "wind_speed_mean",
    "wind_vector_magnitude", "wind_vector_direction",
    "wind_direction_std_dev", "max_wind_speed",
    "heat_units", "eto_reference",
    "etos_reference", "actual_vapor_pressure_mean",
    "dewpoint_mean"
]

def parse_ext_file(file_path):
    """Parses the .ext file and converts it to structured JSON format."""
    data_entries = []

    with open(file_path, "r") as file:
        lines = file.readlines()

    for line in lines:
        # Extract numerical data from each line
        line = line.strip()
        if line and re.match(r"^\d{4},", line):  # Line starts with a 4-digit year
            values = line.split(",")
            if len(values) >= len(COLUMN_NAMES):
                entry = {COLUMN_NAMES[i]: values[i] for i in range(len(COLUMN_NAMES))}
                data_entries.append(entry)

    return data_entries

def save_json(data, output_file):
    """Saves parsed data as a JSON file."""
    with open(output_file, "w") as json_file:
        json.dump(data, json_file, indent=4)

# Example usage
input_file = "/home/tanmayagrawal21/Downloads/0623rd.txt"  # Replace with actual file path
data = parse_ext_file(input_file)
# extract year from data 
year = data[0].get("year")
output_file = f"azmet_output/{year}.json"
save_json(data, output_file)
