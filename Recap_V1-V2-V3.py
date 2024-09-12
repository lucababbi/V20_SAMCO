import pandas as pd
import os

Versions = ["V1", "V2", "V3"]
Price = "Close"
Store_Excel = True
# Dictionary to hold DataFrames
Frame = {}
Shared_Client = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\Client_Shared\Shared_Client.csv", sep=";", parse_dates=["Date"])

# Read Input from SWACALLCAP
MARSEP = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\SWACALLCAP_MARSEP_OPEN.csv", parse_dates=["Date"], index_col=0)
JUNDEC = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\SWACALLCAP_JUNDEC_OPEN.csv", parse_dates=["Date"], index_col=0)
SWACALLCAP = pd.concat([MARSEP, JUNDEC]).rename(columns={"Mcap_Units_Index_Currency": "Mcap_Units_Index_Currency_Open"})

Path = r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output"

# Read each versions
for Version in Versions:
    if Version == "V1":
        CSV = os.path.join(Path, Price, "2024", Version,"Final_Buffer_V20_Cutoff_Mcap_Enhanced_STEP2_2023_DEC_Close_V1.csv")
    else:
        CSV = os.path.join(Path, Price, "2024", Version, "Final_Buffer_V20_Cutoff_Mcap_Enhanced_STEP2_2023_DEC_Close_" + Version + ".csv")

    # Create Frames
    Frame[Version] = pd.read_csv(CSV, index_col=0, parse_dates=["Date"]).sort_values(by="Weight", ascending=False).drop(columns={"Mcap_Units_Index_Currency"})
    Frame[Version] = Frame[Version].merge(SWACALLCAP, on=["Date", "Internal_Number"], how="left")
    Frame[Version]["Weight"] = Frame[Version]["Mcap_Units_Index_Currency_Open"] / Frame[Version]["Mcap_Units_Index_Currency_Open"].sum()
    Frame[Version]["InShared"] = Frame[Version]["Internal_Number"].isin(Shared_Client["internalNumber"])

# Add to the dictionary the Shared_Client Frame
Shared_Client = Shared_Client.merge(SWACALLCAP, left_on=["Date", "internalNumber"], right_on=["Date", "Internal_Number"], how="left")
Shared_Client["Weight"] = Shared_Client["Mcap_Units_Index_Currency_Open"] / Shared_Client["Mcap_Units_Index_Currency_Open"].sum()
Frame["Shared_Client"] = Shared_Client
Versions = ["V1", "V2", "V3", "Shared_Client"]

if Store_Excel == True:
    Excel_Path = r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\Recap\Comparison_Versions.xlsx"

    with pd.ExcelWriter(Excel_Path) as writer:
            for Version in Versions:
                try:
                    Frame[Version].drop(columns={"Capfactor", "Free_Float"}).to_excel(writer, sheet_name=f"{Version}", index=False)
                except:
                    Frame[Version].to_excel(writer, sheet_name=f"{Version}", index=False)