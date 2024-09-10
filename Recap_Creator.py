import pandas as pd
import os
import glob

Versions = ["V1", "V2", "V3"]
Price = "Close"
Store_Excel = False
# Dictionary to hold DataFrames
Frame = {}
Final_Frame = {}
Shared_Client = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\Client_Shared\Shared_Client.csv", sep=";", parse_dates=["Date"])

Path = r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output"

# Read each versions
for Version in Versions:
    if Version == "V1":
        CSV = os.path.join(Path, Price, "2024", Version,"Final_Buffer_V20_Cutoff_Mcap_Enhanced_STEP2_2023_DEC_Close_V1.csv")
    else:
        CSV = os.path.join(Path, Price, "2024", Version, "Final_Buffer_V20_Cutoff_Mcap_Enhanced_STEP2_2023_DEC_Close_" + Version + ".csv")

    # Create Frames
    Frame[Version] = pd.read_csv(CSV, index_col=0, parse_dates=["Date"]).sort_values(by="Weight", ascending=False)

# Loop for each Version
for Version in Versions:
    # Create a copy of the DataFrame for the current version
    current_df = Frame[Version].copy()

    # Initialize columns for weights from other versions
    current_df["Weight_V1"] = 0
    current_df["Weight_V2"] = 0
    current_df["Weight_V3"] = 0

    # Check weights in other DataFrames
    if Version != "V1":
        current_df["Weight_V1"] = current_df["Internal_Number"].map(Frame["V1"].set_index("Internal_Number")["Weight"].to_dict()).fillna(0)
    if Version != "V2":
        current_df["Weight_V2"] = current_df["Internal_Number"].map(Frame["V2"].set_index("Internal_Number")["Weight"].to_dict()).fillna(0)
    if Version != "V3":
        current_df["Weight_V3"] = current_df["Internal_Number"].map(Frame["V3"].set_index("Internal_Number")["Weight"].to_dict()).fillna(0)

    # Store the result in the Final_Frame dictionary
    Final_Frame[Version] = pd.DataFrame({
        "Date": current_df["Date"],
        "Instrument_Name": current_df["Instrument_Name"],
        "Internal_Number": current_df["Internal_Number"],
        "Mcap_Units_Index_Currency": current_df["Mcap_Units_Index_Currency"],
        "Weight": current_df["Weight"],
        "Weight_V1": current_df["Weight_V1"],
        "Weight_V2": current_df["Weight_V2"],
        "Weight_V3": current_df["Weight_V3"],
        "InV1": current_df["Internal_Number"].isin(Frame["V1"]["Internal_Number"]),
        "InV2": current_df["Internal_Number"].isin(Frame["V2"]["Internal_Number"]),
        "InV3": current_df["Internal_Number"].isin(Frame["V3"]["Internal_Number"]),
        "InClientShared": current_df["Internal_Number"].isin(Shared_Client["internalNumber"])
    })

    # Drop Weight_Version as it will result empty
    if Version == "V1":
        Final_Frame[Version] = Final_Frame[Version].drop(columns={"Weight_V1", "InV1"}) 
    elif Version == "V2":
        Final_Frame[Version] = Final_Frame[Version].drop(columns={"Weight_V2", "InV2"}) 
    elif Version == "V3":
        Final_Frame[Version] = Final_Frame[Version].drop(columns={"Weight_V3", "InV3"}) 

# Sharable Table for SHELL
Sharable_Table = pd.DataFrame(columns=[
                                        "Version",
                                        "Count",
                                        "NotInV20",
                                        "Total_Weight"
                                      ])

# Read Open MCAP from SWACALLCAP
# Read Input from SWACALLCAP
MARSEP = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\SWACALLCAP_MARSEP_OPEN.csv", parse_dates=["Date"], index_col=0)
JUNDEC = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\SWACALLCAP_JUNDEC_OPEN.csv", parse_dates=["Date"], index_col=0)
SWACALLCAP = pd.concat([MARSEP, JUNDEC]).rename(columns={"Mcap_Units_Index_Currency": "Mcap_Units_Index_Currency_Open"})

for Version in Versions:
    Final_Frame[Version] = Final_Frame[Version].merge(SWACALLCAP, on=["Date", "Internal_Number"], how="left")
    Final_Frame[Version]["Open_Weight"] = Final_Frame[Version]["Mcap_Units_Index_Currency_Open"] / Final_Frame[Version]["Mcap_Units_Index_Currency_Open"].sum()

    temp_Sharable_Table = pd.DataFrame({"Version": Version,
                                        "Count": len(Final_Frame[Version]),
                                        "NotInV20": [len(Final_Frame[Version][~Final_Frame[Version]["Internal_Number"].isin(Shared_Client["internalNumber"])])],
                                        "Total_Weight": [Final_Frame[Version][~Final_Frame[Version]["Internal_Number"].isin(Shared_Client["internalNumber"])]["Open_Weight"].sum()]
                                      })
    
    Sharable_Table = pd.concat([Sharable_Table, temp_Sharable_Table])

# Store the result
Sharable_Table.to_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\Recap\Table_Sharable_Recap.csv", index=False)

if Store_Excel == True:
    Excel_Path = r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\Recap\Recap_Versions.xlsx"

    with pd.ExcelWriter(Excel_Path) as writer:
            for Version in Versions:
                Final_Frame[Version].to_excel(writer, sheet_name=f"{Version}", index=False)