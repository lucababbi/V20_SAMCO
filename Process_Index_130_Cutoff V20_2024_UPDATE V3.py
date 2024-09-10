import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import numpy as np
import pandasql
from pandasql import sqldf
import polars as pl

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

# Read CSV file for Cutoff dates (Market Cap)
Securities_Cutoff_MARSEP = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Security_Cutoff\Output_Securities_Cutoff_MARSEP_NEW.csv",
                                       sep=",", parse_dates=["validDate"], index_col=0).rename(columns={"stoxxId": "stoxxId_Cutoff", "currency": "currency_Cutoff",
                                                                                                        "closePrice": "closePrice_Cutoff", "freeFloat": "freeFloat_Cutoff",
                                                                                                        "shares": "shares_Cutoff", "Capfactor": "Capfactor_Cutoff"})

Securities_Cutoff_MARSEP_2024 = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Security_Cutoff\Output_Securities_Cutoff_MARSEP_NEW_2024.csv",
                                       sep=",", parse_dates=["validDate"], index_col=0).rename(columns={"stoxxId": "stoxxId_Cutoff", "currency": "currency_Cutoff",
                                                                                                        "closePrice": "closePrice_Cutoff", "freeFloat": "freeFloat_Cutoff",
                                                                                                        "shares": "shares_Cutoff", "Capfactor": "Capfactor_Cutoff"})

# Merge the old and new Input
Securities_Cutoff_MARSEP = pd.concat([Securities_Cutoff_MARSEP, Securities_Cutoff_MARSEP_2024])
                                       
Securities_Cutoff_MARSEP = Securities_Cutoff_MARSEP.dropna(subset=["stoxxId_Cutoff"])
                                       
Securities_Cutoff_JUNDEC = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Security_Cutoff\Output_Securities_Cutoff_JUNDEC_NEW.csv",
                                       sep=",", parse_dates=["validDate"], index_col=0).rename(columns={"stoxxId": "stoxxId_Cutoff", "currency": "currency_Cutoff",
                                                                                                        "closePrice": "closePrice_Cutoff", "freeFloat": "freeFloat_Cutoff",
                                                                                                        "shares": "shares_Cutoff", "Capfactor": "Capfactor_Cutoff"})

Securities_Cutoff_JUNDEC_2024 = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Security_Cutoff\Output_Securities_Cutoff_JUNDEC_NEW_2024.csv",
                                       sep=",", parse_dates=["validDate"], index_col=0).rename(columns={"stoxxId": "stoxxId_Cutoff", "currency": "currency_Cutoff",
                                                                                                        "closePrice": "closePrice_Cutoff", "freeFloat": "freeFloat_Cutoff",
                                                                                                        "shares": "shares_Cutoff", "Capfactor": "Capfactor_Cutoff"})

# Merge the old and new Input
Securities_Cutoff_JUNDEC = pd.concat([Securities_Cutoff_JUNDEC, Securities_Cutoff_JUNDEC_2024])

Securities_Cutoff_JUNDEC = Securities_Cutoff_JUNDEC.dropna(subset=["stoxxId_Cutoff"])

Securities_Cutoff = pd.concat([Securities_Cutoff_MARSEP, Securities_Cutoff_JUNDEC])

# Read CSV files for Turnover
Turnover = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Turnover\Turnover_SID.parquet")
# Drop unuseful columns
Turnover = Turnover.drop(["vd", "calcType", "token"])
# Keep only relevant fields
Turnover = Turnover.filter(pl.col("field").is_in(["TurnoverRatioFO", "TurnoverRatioFO_India1"]))

# Transform the table
Turnover = Turnover.pivot(
                values="Turnover_Ratio",
                index=["Date", "Internal_Number"],
                on="field"
                ).rename({"TurnoverRatioFO": "Turnover_Ratio"})

# Fill NA in TurnoverRatioFO_India1
Turnover = Turnover.with_columns(
                                pl.col("TurnoverRatioFO_India1").fill_null(pl.col("Turnover_Ratio"))
                                ).drop("Turnover_Ratio").rename({"TurnoverRatioFO_India1": "Turnover_Ratio"}).to_pandas()

Turnover["Date"] = pd.to_datetime(Turnover["Date"])

# Define parameters for Turnover
New = 0.15
Old = 0.10
# Define parameter for Market Cap
Threshold = 0.99
Threshold_Korea = 0.65

# Parameter for FOR
Threshold_FOR = 0.15
FOR_Removed = pd.DataFrame()

# Version
Version = "20_Cutoff_Mcap_Enhanced"   

Output = pd.DataFrame()
Filtered = pd.DataFrame()

# Add Turnover Information
Input = Input.merge(Turnover, on=["Date", "Internal_Number"], how="left")

# Set Turnover equal to 0 where NaN
Input["Turnover_Ratio"] = Input["Turnover_Ratio"].fillna(0)

# Add FX_Rate as of Cutoff
FX_Rate = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\FX\FX_Historical_UPDATE.csv", index_col=0, parse_dates=["cutoff_date"])
FX_Rate_2024 = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\FX\FX_Historical_UPDATE_2024.csv", index_col=0, parse_dates=["cutoff_date"])

# Merge the two Frames with Input
FX_Rate = pd.concat([FX_Rate, FX_Rate_2024])

Input = Input.merge(FX_Rate, left_on=["Cutoff", "Currency"], right_on=["cutoff_date", "frm_currency"]).drop(columns={"frm_currency", 
                            "to_currency", "cutoff_date"}).rename(columns={"exchange_rate": "FX_Rate_Cutoff"})

# Add information as of Cutoff
Input = Input.merge(Securities_Cutoff, left_on=["Cutoff", "Internal_Number"], 
        right_on=["validDate", "stoxxId_Cutoff"], how="left").drop(columns={"validDate", "stoxxId_Cutoff", "currency_Cutoff", "Capfactor_Cutoff"})

# Calculate Full Market Cap
Input["Full_Market_Cap_Review"] = Input["Mcap_Units_Index_Currency"] / Input["Free_Float"]

# Calculate USD Price at Cutoff
Input["Close_USD_Cutoff"] = Input["closePrice_Cutoff"] * Input["FX_Rate_Cutoff"]

# Calculate Free Float Market Cap as of Cutoff
Input["Free_Float_Market_Cutoff"] = Input["Close_USD_Cutoff"] * Input["shares_Cutoff"] * Input["Free_Float"] * Input["Capfactor"]

# Calculate Full Market Cap as of Cutoff
Input["Full_Market_Cap_Cutoff"] = Input["Free_Float_Market_Cutoff"] / Input["Free_Float"] / Input["Capfactor"]

# Foreign Ownership Restriction adjusted Free Float Market Cap
Input["FOR_FF"] = Input["Free_Float"] * Input["Capfactor"]

# Apply specific rule for CHINA A securities
mask = (Input["Country"] == "CN") & (Input["Instrument_Name"].str.contains("\'A\'") | Input["Instrument_Name"].str.contains("(CCS)"))

# Calculate the values based on the conditions
values = Input[mask]["Free_Float"] * Input[mask]["Capfactor"] / 0.2

# Update the "FOR_FF" column only for rows meeting the conditions
Input.loc[mask, "FOR_FF"] = values

# Round to the nearest 1%
Input["FOR_FF"] = np.round(Input["FOR_FF"] / 0.01) * 0.01

# Review Process    
for date in Dates_Frame["Cutoff"]:
    if pd.to_datetime(date).strftime("%Y-%m-%d") == pd.to_datetime(Dates_Frame.head(1)["Cutoff"].values[0]).strftime("%Y-%m-%d"):
        temp_Input = Input.query("Cutoff == @date")

        # Apply Turnover Ratio filter
        temp_Input = temp_Input.query("Turnover_Ratio > @New")

        if date <= pd.to_datetime("2023-03-20"):
            # Remove 'A'-CCS from Small Cap Universe
            temp_Input = temp_Input.query('\
                ~((Country == "CN") \
                and (\
                    Instrument_Name.str.contains("\'A\'") \
                    or Instrument_Name.str.contains("(CCS)")\
                ) \
                and (Index_Symbol == "SWESCGV"))')
        else:
            temp_Input = temp_Input.query("Exchange != 'Stock Exchange of Hong Kong - SSE Securities' and Exchange != 'Stock Exchange of Hong Kong - SZSE Securities'")
        
        # Add removed securities to the Frame
        temp_Removed = temp_Input[temp_Input["FOR_FF"] < Threshold_FOR]

        # Filter for Foreign Ownership Restriction adjusted Free Float Market Cap
        temp_Input = temp_Input[temp_Input["FOR_FF"] >= Threshold_FOR]

        # Filter for each country in the Input
        for country in temp_Input["Country"].unique():
            temp = temp_Input.query("Country == @country")

            temp = temp.sort_values(["Free_Float_Market_Cutoff"], ascending=False)
            temp["Cumulative_Free_Float_Market_Cap_Cutoff"] = temp["Free_Float_Market_Cutoff"].cumsum()
            temp["Rank"] = temp["Cumulative_Free_Float_Market_Cap_Cutoff"] / temp["Free_Float_Market_Cutoff"].sum()

            if country == "KR":
                temp = pd.concat([temp[temp["Rank"] < Threshold_Korea], temp[temp["Rank"] >= Threshold_Korea].iloc[0:1]])
            else:
                temp = pd.concat([temp[temp["Rank"] < Threshold], temp[temp["Rank"] >= Threshold].iloc[0:1]])

            Filtered = pd.concat([Filtered,temp])

        # Assign the Filtered DataFrame to temp_Input
        temp_Input = Filtered

        temp_Input["Weight"] = temp_Input["Mcap_Units_Index_Currency"] / temp_Input["Mcap_Units_Index_Currency"].sum()

        temp_Input["Prev_Comp"] = False
        temp_Input["Index_Component_Count"] = len(temp_Input)
        Output = pd.concat([Output, temp_Input])
        Prev_Comp = temp_Input

        FOR_Removed = pd.concat([FOR_Removed, temp_Removed])

    else:
        temp_Input = Input.query("Cutoff == @date")

        temp_Input["Prev_Comp"] = temp_Input["InfoCode"].isin(Prev_Comp["InfoCode"])

        # Apply Turnover Ratio filter
        temp_Input = temp_Input.query("(Prev_Comp == True & Turnover_Ratio > @Old) | (Prev_Comp == False & Turnover_Ratio > @New)")

        if date <= pd.to_datetime("2023-03-20"):
            # Remove 'A'-CCS from Small Cap Universe
            temp_Input = temp_Input.query('\
                ~((Country == "CN") \
                and (\
                    Instrument_Name.str.contains("\'A\'") \
                    or Instrument_Name.str.contains("(CCS)")\
                ) \
                and (Index_Symbol == "SWESCGV"))')
        else:
            temp_Input = temp_Input.query("Exchange != 'Stock Exchange of Hong Kong - SSE Securities' and Exchange != 'Stock Exchange of Hong Kong - SZSE Securities'")

        # Add removed securities to the Frame
        temp_Removed = temp_Input[temp_Input["FOR_FF"] < Threshold_FOR]

        # Filter for Foreign Ownership Restriction adjusted Free Float Market Cap
        temp_Input = temp_Input[temp_Input["FOR_FF"] >= Threshold_FOR]
        
        # Reset Filtered DataFrame
        Filtered = pd.DataFrame()

        # Filter for each country in the Input
        for country in temp_Input["Country"].unique():
            temp = temp_Input.query("Country == @country")

            temp = temp.sort_values(["Free_Float_Market_Cutoff"], ascending=False)
            temp["Cumulative_Free_Float_Market_Cap_Cutoff"] = temp["Free_Float_Market_Cutoff"].cumsum()
            temp["Rank"] = temp["Cumulative_Free_Float_Market_Cap_Cutoff"] / temp["Free_Float_Market_Cutoff"].sum()

            if country == "KR":
                temp = pd.concat([temp[temp["Rank"] < Threshold_Korea], temp[temp["Rank"] >= Threshold_Korea].iloc[0:1]])
            else:
                temp = pd.concat([temp[temp["Rank"] < Threshold], temp[temp["Rank"] >= Threshold].iloc[0:1]])

            Filtered = pd.concat([Filtered,temp])

        # Assign the Filtered DataFrame to temp_Input
        temp_Input = Filtered

        temp_Input["Weight"] = temp_Input["Mcap_Units_Index_Currency"] / temp_Input["Mcap_Units_Index_Currency"].sum()

        Prev_Comp = temp_Input
        temp_Input["Index_Component_Count"] = len(temp_Input)  
        Output = pd.concat([Output, temp_Input])

        FOR_Removed = pd.concat([FOR_Removed, temp_Removed])

Output[[
    "Date",
    "Index_Component_Count",
    "Internal_Number",
    "ISIN",
    "SEDOL",
    "Instrument_Name",
    "Country",
    "Currency",
    "Exchange",
    "ICB",
    "Mcap_Units_Index_Currency",
    "InfoCode",
    "Close_USD_Cutoff",
    "shares_Cutoff",
    "freeFloat_Cutoff",
    "Free_Float",
    "Capfactor",
    "Free_Float_Market_Cutoff",
    "Full_Market_Cap_Cutoff",
    "FOR_FF"
]].to_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\V3_Base\Final_Buffer_V" + str(Version) + "_2024.csv")

# Add Full MCAP as of Review
FOR_Removed["Full_Mcap_Units_Index_Currency"] = FOR_Removed["Shares"] * FOR_Removed["Close_unadjusted_local"] * FOR_Removed["FX_local_to_Index_Currency"]

FOR_Removed[[
    "Date",
    "Internal_Number",
    "InfoCode",
    "ISIN",
    "SEDOL",
    "Instrument_Name",
    "Country",
    "Currency",
    "ICB",
    "Mcap_Units_Index_Currency",
    "Full_Mcap_Units_Index_Currency",
    "Free_Float",
    "Capfactor",
    "FOR_FF",
    "Weight"
]].to_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\V3_Base\Final_Buffer_V" + str(Version) + "_FOR_Removed_Securities_2024.csv")