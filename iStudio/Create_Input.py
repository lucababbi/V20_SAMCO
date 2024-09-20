import pandas as pd
import os
from datetime import datetime

Versions = ["V5"]
Price = "Close"

Path = r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output"

# Capfactor from SWACALLCAP
CapFactor = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\Capfactor_SWACALLCAP_MAR2019.csv", parse_dates=["Date"], index_col=0)

# Read each versions
for Version in Versions:

    if Version == "V1":
        CSV = os.path.join(Path, Price, "2024", Version, "Sharable_Final_Buffer_V20_Cutoff_Mcap_Enhanced_STEP2_Close_V1.csv")
    else:
        CSV = os.path.join(Path, Price, "2024", Version, "Sharable_Final_Buffer_V20_Cutoff_Mcap_Enhanced_STEP2_Close_" + Version + ".csv")

    Frame = pd.read_csv(CSV, index_col=0, parse_dates=["Date"])
    
    # Filter for needed columns
    Frame = Frame[["Internal_Number", "SEDOL", "ISIN", "Date"]]
    # Create weightFactor
    Frame["weightFactor"] = 1

    # Add CapFactor from SWACALLCAP
    Frame = Frame.merge(CapFactor[["Date", "Internal_Number", "Capfactor"]], on=["Date", "Internal_Number"], how="left")

    # Convert column Date
    Frame["Date"] = Frame["Date"].dt.strftime('%Y%m%d')

    # Renaming to convention
    Frame = Frame.rename(columns={"Internal_Number": "STOXXID", "Date": "effectiveDate", "Capfactor": "capFactor"})

    # Store the .CSV
    # Get current date formatted as YYYYMMDD_HHMMSS
    current_datetime = datetime.today().strftime('%Y%m%d')

    # Store the .CSV with version and timestamp
    Frame.to_csv(
        os.path.join(
            r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\iStudio\Output", 
            str(Version) + "_" + current_datetime + ".csv"
        ), 
        index=False, 
        lineterminator="\n", 
        sep=";"
    )