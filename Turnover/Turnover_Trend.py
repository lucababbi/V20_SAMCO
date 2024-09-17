import polars as pl
import pandas as pd
from pandasql import sqldf

Version = "V1"

# Universe
InfoCode = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\InfoCode.csv", parse_dates=["vf", "vt"])
# Deal with 99991230 dates with a date in remote future
InfoCode["vt"] = InfoCode["vt"].replace("99991230", "21001230")
# Convert columns into DateTime
InfoCode["vt"] = pd.to_datetime(InfoCode["vt"], format = "%Y%m%d")
# Infocode = InfoCode.loc[InfoCode.groupby("StoxxId")["vt"].idxmax()]

Dates_Frame = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Dates\Review_Date-MAR-SEP.csv", index_col=0, parse_dates=["Review", "Cutoff"])
Dates_Frame_JUNDEC = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Dates\Review_Date-JUN-DEC.csv", index_col=0, parse_dates=["Review", "Cutoff"])

# Concat the two Review Frames
Dates_Frame = pd.concat([Dates_Frame, Dates_Frame_JUNDEC]).sort_values(by="Review").reset_index(inplace=False).drop(columns={"index"})

Cleaned_Frame = pd.DataFrame()

# Read CSV file, parse dates, handle NA values, drop rows with NA, and specify dtype
PRE = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\Updated_Universe\PRE_MAR_2023.csv",
                    index_col=0, parse_dates=["Date"])

# Remove empty/null Securities
PRE = PRE.query("Mcap_Units_Index_Currency > 0")

# Re-arrange PRE Frame to adapt to newer one
PRE = PRE[["Date", "Index_Symbol", "Internal_Number", "ISIN", "SEDOL", "RIC", "Instrument_Name", "Country", "Currency", "exchange", 
           "ICB", "Shares", "Free_Float", "Capfactor", "Close_unadjusted_local", "FX_local_to_Index_Currency", "Mcap_Units_Index_Currency", "Weight"]].rename(columns={"exchange": "Exchange"})
    
POST = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\Updated_Universe\POST_MAR_2023.csv",
                    parse_dates=["Date"], index_col=0)

# Remove empty/null Securities
POST = POST.query("Mcap_Units_Index_Currency > 0")

# Remove unuseful columns
POST = POST[["Date", "Index_Symbol", "Internal_Number", "ISIN", "SEDOL", "RIC", "Instrument_Name", "Country", "Currency", "Exchange",
             "ICB", "Shares", "Free_Float", "Capfactor", "Close_unadjusted_local", "FX_local_to_Index_Currency", "Mcap_Units_Index_Currency", "Weight"]]

# Concat the two Inputs
Input = pd.concat([PRE, POST])

# Add Cutoff information
Input = Input.merge(Dates_Frame, left_on="Date", right_on="Review", how="left").drop(columns={"Review"})

# Add information of InfoCode
Input = sqldf("""
                     SELECT * FROM Input AS Input
                     LEFT JOIN InfoCode AS Info
                     ON Info.StoxxId = Input.Internal_Number
                     WHERE Input.Date >= Info.vf
                     AND Input.Date <= Info.vt                     
                    """
                    )

Input["Date"] = pd.to_datetime(Input["Date"])
Input["Cutoff"] = pd.to_datetime(Input["Cutoff"])
Input = Input.drop(columns={"InfoCodeSource", "SecCode", "SecCodeRegion", "SecCodeSource", "vf", "vt", "SecId","Sedol6", "Isin", "Ric"})
Input = Input.dropna(subset="InfoCode")

if Version == "V1":

    # Read CSV files for Turnover
    Turnover = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Turnover\Output_Turnover_Cutoff_3M_MARSEP.csv", 
                        parse_dates=["Date"], dtype={"InfoCode": "int64"})

    Turnover_2024 = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Turnover\Output_Turnover_Cutoff_3M_MARSEP_2024.csv", 
                        parse_dates=["Date"], dtype={"InfoCode": "int64"})

    # Merge the old and new Input
    Turnover = pd.concat([Turnover, Turnover_2024])

    Turnover_JUNDEC = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Turnover\Output_Turnover_Cutoff_3M_JUNDEC.csv",
                        parse_dates=["Date"], dtype={"InfoCode": "int64"})

    Turnover_JUNDEC_2024 = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Turnover\Output_Turnover_Cutoff_3M_JUNDEC_2024.csv",
                        parse_dates=["Date"], dtype={"InfoCode": "int64"})

    # Merge the old and new Input
    Turnover_JUNDEC = pd.concat([Turnover_JUNDEC, Turnover_JUNDEC_2024])

    # Merget the two Frame with Turnover
    Turnover = pd.concat([Turnover, Turnover_JUNDEC]).sort_values(by="Date")

    # Add Turnover Information
    Input = Input.merge(Turnover[["InfoCode", "Turnover_Ratio", "Date", "Start", "End"]], left_on=["InfoCode", "Date"], 
                                right_on=["InfoCode", "Date"], how="left")

    # Set Turnover equal to 0 where NaN
    Input["Turnover_Ratio"] = Input["Turnover_Ratio"].fillna(0)

elif Version == "V2":
    # Read CSV files for Turnover
    Turnover = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Turnover\Turnover_SID.parquet")
    # Drop unuseful columns
    Turnover = Turnover.drop(["vd", "calcType", "token"])
    # Keep only relevant fields
    Turnover = Turnover.filter(pl.col("field").is_in(["TurnoverRatioFO"]))

    # Transform the table
    Turnover = Turnover.pivot(
                    values="Turnover_Ratio",
                    index=["Date", "Internal_Number"],
                    on="field"
                    ).rename({"TurnoverRatioFO": "Turnover_Ratio"}).to_pandas()

    Turnover["Date"] = pd.to_datetime(Turnover["Date"])

    # Add Turnover Information
    Input = Input.merge(Turnover, on=["Date", "Internal_Number"], how="left")

    # Set Turnover equal to 0 where NaN
    Input["Turnover_Ratio"] = Input["Turnover_Ratio"].fillna(0)

# Remove unneeded columns
Input = Input[["Date", "Internal_Number", "Instrument_Name", "Country", "Turnover_Ratio", "Weight", "Mcap_Units_Index_Currency"]].query("Date >= '2019-03-18'")

# Sharable Version V1
Shared_V1 = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\V1_Base\Final_Buffer_V20_Cutoff_Mcap_Enhanced_2024.csv", parse_dates=["Date"], index_col=0)

# Add Turnover Ratio
Shared_V1 = Shared_V1.merge(Turnover[["InfoCode", "Turnover_Ratio", "Date", "Start", "End"]], left_on=["InfoCode", "Date"], 
                                right_on=["InfoCode", "Date"], how="left")

# Recalculate Weight
Shared_V1 = Shared_V1[["Date", "Internal_Number", "Instrument_Name", "Country", "Turnover_Ratio", "Mcap_Units_Index_Currency"]].query("Date >= '2019-03-18'")

# Frame Output
Turnover_Trend = pd.DataFrame()

# Calculate the AVG Turnover for each Date
for date in Input.Date.unique():
    temp_Input = Input.query("Date == @date")
    temp_Shared_V21 = Shared_V1.query("Date == @date")

    # Rebase the Weights
    temp_Input["Weight"] = temp_Input["Mcap_Units_Index_Currency"] / temp_Input["Mcap_Units_Index_Currency"].sum()
    temp_Shared_V21["Weight"] = temp_Shared_V21["Mcap_Units_Index_Currency"] / temp_Shared_V21["Mcap_Units_Index_Currency"].sum()

    # Keep only CN securities
    temp_Input = temp_Input.query("Country == 'CN'")
    temp_Shared_V21 = temp_Shared_V21.query("Country == 'CN'")

    # Weighted ToR
    temp_Input["Weighted_TOR"] = temp_Input["Turnover_Ratio"] * temp_Input["Weight"]

    temp_Trend = pd.DataFrame({
                                "Date": [date],
                                "CN_Weight_Step1": [temp_Shared_V21["Weight"].sum()],
                                "CN_Weight": [temp_Input["Weight"].sum()],
                                "Equally_Weighted_Average_TOR": [temp_Input["Turnover_Ratio"].mean()],
                                "Weighted_Average_ToR": [temp_Input["Weighted_TOR"].mean()]
                             })
    
    Turnover_Trend = pd.concat([Turnover_Trend, temp_Trend])

Turnover_Trend.to_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Turnover\Turnover_Trend_CN_Weighted.csv", index=False)