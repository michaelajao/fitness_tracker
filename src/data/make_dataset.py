import os
import glob
import pandas as pd

# --------------------------------------------------------------
# Read single CSV file
# --------------------------------------------------------------
# single_file_acc = pd.read_csv(
#     "../../data/raw/MetaMotion/A-bench-heavy2-rpe8_MetaWear_2019-01-11T16.10.08.270_C42732BE255C_Accelerometer_12.500Hz_1.4.4.csv"
# )

# single_file_gyr = pd.read_csv(
#     "../../data/raw/MetaMotion/A-bench-heavy2-rpe8_MetaWear_2019-01-11T16.10.08.270_C42732BE255C_Gyroscope_25.000Hz_1.4.4.csv"
# )
# # --------------------------------------------------------------
# # List all data in data/raw/MetaMotion
# # --------------------------------------------------------------
# files = glob("../../data/raw/MetaMotion/*.csv")
# len(files)

# # --------------------------------------------------------------
# # Extract features from filename
# # --------------------------------------------------------------
# data_path = "../../data/raw/MetaMotion/"
# f = files[0]


# participant_path = f.split("-")[0].replace(data_path, "")
# participant = os.path.basename(participant_path)
# label = f.split("-")[1]
# category = f.split("-")[2].rstrip("123")

# df = pd.read_csv(f)

# df["participant"] = participant
# df["label"] = label
# df["category"] = category

# --------------------------------------------------------------
# Read all files
# --------------------------------------------------------------


def process_files(files, data_path, set_type):
    """Process a list of files and return a DataFrame with a set number."""
    data_frames = []
    set_number = 1
    for file in files:
        df_temp = pd.read_csv(file)
        participant, label, category = extract_info(file, data_path)
        df_temp["participant"] = participant
        df_temp["label"] = label
        df_temp["category"] = category
        df_temp["set"] = set_number
        data_frames.append(df_temp)
        set_number += 1
    return pd.concat(data_frames, ignore_index=True)


def extract_info(filename, data_path):
    """Extract participant, label, and category from a filename."""
    relative_path = os.path.relpath(filename, data_path)
    parts = relative_path.split("-")
    participant = parts[0]
    label = parts[1]
    category = parts[2].split("_")[0].rstrip("1234567890").rstrip("_MetaWear_2019")
    return participant, label, category


def process_dataframe(df):
    """Set the index to datetime format and remove specified columns."""
    df.index = pd.to_datetime(df["epoch (ms)"], unit="ms")
    df.drop(columns=["epoch (ms)", "time (01:00)", "elapsed (s)"], inplace=True)
    return df


# Paths
data_path = "../../data/raw/MetaMotion/"
acc_files = glob.glob(os.path.join(data_path, "*Accelerometer*.csv"))
gyr_files = glob.glob(os.path.join(data_path, "*Gyroscope*.csv"))

# Process files
df_acc = process_files(acc_files, data_path, "Accelerometer")
df_gyr = process_files(gyr_files, data_path, "Gyroscope")


# Process dataframes
df_acc = process_dataframe(df_acc)
df_gyr = process_dataframe(df_gyr)


data_merged = pd.concat([df_acc.iloc[:, :3], df_gyr], axis=1)

# df_acc[df_acc["set"] == 1]

# --------------------------------------------------------------
# Working with datetimes
# --------------------------------------------------------------

# df_acc.info()
# pd.to_datetime(df_acc["epoch (ms)"], unit="ms")

# df_acc.index = pd.to_datetime(df_acc["epoch (ms)"], unit="ms")
# df_gyr.index = pd.to_datetime(df_gyr["epoch (ms)"], unit="ms")

# del df_acc["epoch (ms)"]
# del df_acc["time (01:00)"]
# del df_acc["elapsed (s)"]

# del df_gyr["epoch (ms)"]
# del df_gyr["time (01:00)"]
# del df_gyr["elapsed (s)"]
# --------------------------------------------------------------
# Turn into function
# --------------------------------------------------------------


# --------------------------------------------------------------
# Merging datasets
# --------------------------------------------------------------
data_merged = pd.concat([df_acc.iloc[:, :3], df_gyr], axis=1)

data_merged.columns = [
    "acc_x",
    "acc_y",
    "acc_z",
    "gyr_x",
    "gyr_y",
    "gyr_z",
    "participant",
    "label",
    "category",
    "set",
]


data_merged.columns
# --------------------------------------------------------------
# Resample data (frequency conversion)
# --------------------------------------------------------------

# Accelerometer:    12.500HZ
# Gyroscope:        25.000Hz


sampling = {
    "acc_x": "mean",
    "acc_y": "mean",
    "acc_z": "mean",
    "gyr_x": "mean",
    "gyr_y": "mean",
    "gyr_z": "mean",
    "label": "last",
    "category": "last",
    "participant": "last",
    "set": "last",
}

data_merged[:1000].resample(rule="200ms").apply(sampling)

days = [g for n, g in data_merged.groupby(pd.Grouper(freq="D"))]

data_resampled = pd.concat(
    [df.resample(rule="200ms").apply(sampling).dropna() for df in days]
)

data_resampled.info()

data_resampled["set"] = data_resampled["set"].astype("int")
# --------------------------------------------------------------
# Export dataset
# --------------------------------------------------------------
data_resampled.to_pickle("../../data/interim/01_data_processed.pkl")