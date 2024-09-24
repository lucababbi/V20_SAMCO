import pandas as pd
from datetime import datetime
import os

Version = "V10"
Step_2 = True
Capping = 0.075

# Read V25 Composition
V25 = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\Close\2024\V5\Sharable_Final_Buffer_V20_Cutoff_Mcap_Enhanced_STEP2_Close_V5.csv",
                    parse_dates=["Date"], sep=",", index_col=0)[["Date", "ISIN", "SEDOL", "Internal_Number", "Country", "Weight"]]

# Capfactor from SWACALLCAP
CapFactor = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\Capfactor_SWACALLCAP_MAR2019.csv", parse_dates=["Date"], index_col=0)

# Create weightFactor
V25["weightFactor"] = 1

Output = pd.DataFrame(columns=["Date", "CapFactor_Adjustment"]) 

for date in V25.Date.unique():
    temp_V25 = V25.query("Date == @date")

    # Initialize CF_Adjustment with None (or 1.0 if you prefer a default value)
    CF_Adjustment = None

    # Check if total weight for CN is > 10%
    if temp_V25.query("Country == 'CN'")["Weight"].sum() > Capping:
        # Calculate CapFactor Adjustment
        CF_Adjustment = (Capping * temp_V25.query("Country != 'CN'")["Weight"].sum()) / ((1- Capping) * temp_V25.query("Country == 'CN'")["Weight"].sum())

    else:
        CF_Adjustment = 1

    # Create a temporary DataFrame for the current date
    temp_Output = pd.DataFrame([{
        "Date": date,
        "CapFactor_Adjustment": CF_Adjustment
    }])
    
    # Append temp_Output to the main Output DataFrame
    Output = pd.concat([Output, temp_Output], ignore_index=True)

# Create the iStudio Output
# Add CapFactor from SWACALLCAP
V25 = V25.merge(CapFactor[["Date", "Internal_Number", "Capfactor"]], on=["Date", "Internal_Number"], how="left")

Final_Output = pd.DataFrame()

# Adjust the CN CapFactor
for date in V25.Date.unique():
    temp_V25 = V25.query("Date == @date")

    # Multiply the existing Capfactor by the corresponding CapFactor_Adjustment from Output
    temp_V25.loc[temp_V25["Country"] == "CN", "Capfactor"] *= Output.loc[Output["Date"] == date, "CapFactor_Adjustment"].values[0]

    Final_Output = pd.concat([Final_Output, temp_V25])

# Convert column Date
Final_Output["Date"] = Final_Output["Date"].dt.strftime('%Y%m%d')

# Renaming to convention
Final_Output = Final_Output.rename(columns={"Internal_Number": "STOXXID", "Date": "effectiveDate", "Capfactor": "capFactor"})

# Store the .CSV
# Get current date formatted as YYYYMMDD_HHMMSS
current_datetime = datetime.today().strftime('%Y%m%d')

# Store the .CSV with version and timestamp
Final_Output[["STOXXID", "SEDOL", "ISIN", "effectiveDate", "weightFactor", "capFactor"]].to_csv(
    os.path.join(
        r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\iStudio\Output", 
        str(Version) + "_" + current_datetime + "_STEP_2" + str(Step_2) +".csv"
    ), 
    index=False, 
    lineterminator="\n", 
    sep=";"
)