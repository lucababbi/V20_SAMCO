import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import numpy as np
import pandasql
from pandasql import sqldf

# ================================================
#              Open -  Close Price
# ================================================
Price = "Close"
FX_Rate_T_1 = False
Base = "V3"

InfoCode = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\InfoCode.csv", parse_dates=["vf", "vt"])
# Deal with 99991230 dates with a date in remote future
InfoCode["vt"] = InfoCode["vt"].replace("99991230", "21001230")
# Convert columns into DateTime
InfoCode["vt"] = pd.to_datetime(InfoCode["vt"], format = "%Y%m%d")
# Infocode = InfoCode.loc[InfoCode.groupby("StoxxId")["vt"].idxmax()]

SEDOLs = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\SEDOLsMapping.csv", parse_dates=["Date"], index_col=0)

Dates_Frame = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Dates\Review_Date-MAR-SEP.csv", index_col=0, parse_dates=["Review", "Cutoff"])
Dates_Frame_JUNDEC = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Dates\Review_Date-JUN-DEC.csv", index_col=0, parse_dates=["Review", "Cutoff"])

# Concat the two Review Frames
Dates_Frame = pd.concat([Dates_Frame, Dates_Frame_JUNDEC]).sort_values(by="Review").reset_index(inplace=False).drop(columns={"index"})

Final_Frame = pd.DataFrame()

# Read Input from SWACALLCAP
MARSEP = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\SWACALLCAP_MARSEP_OPEN.csv", parse_dates=["Date"], index_col=0)
JUNDEC = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\SWACALLCAP_JUNDEC_OPEN.csv", parse_dates=["Date"], index_col=0)
SWACALLCAP = pd.concat([MARSEP, JUNDEC]).rename(columns={"Mcap_Units_Index_Currency": "Mcap_Units_Index_Currency_Open"})

# Read CSV file, parse dates, handle NA values, drop rows with NA, and specify dtype

# MID Cap Universe
MID_MARSEP = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\SWEMCGV_MARSEP.csv", parse_dates=["Date"], index_col=0)
# Add information of InfoCode

MID_MARSEP = sqldf("""
                     SELECT * FROM MID_MARSEP AS Input
                     LEFT JOIN InfoCode AS Info
                     ON Info.StoxxId = Input.Internal_Number
                     WHERE Input.Date >= Info.vf
                     AND Input.Date <= Info.vt                     
                    """
                    )

MID_MARSEP["Date"] = pd.to_datetime(MID_MARSEP["Date"])
MID_MARSEP = MID_MARSEP.drop(columns={"InfoCodeSource", "SecCode", "SecCodeRegion", "SecCodeSource", "vf", "vt", "SecId","Sedol6", "Isin", "Ric"})
MID_MARSEP = MID_MARSEP.dropna(subset="InfoCode")


MID_JUNDEC = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\SWEMCGV_JUNDEC.csv", parse_dates=["Date"], index_col=0)
MID_JUNDEC = sqldf("""
                     SELECT * FROM MID_JUNDEC AS Input
                     LEFT JOIN InfoCode AS Info
                     ON Info.StoxxId = Input.Internal_Number
                     WHERE Input.Date >= Info.vf
                     AND Input.Date <= Info.vt                     
                    """
                    )

MID_JUNDEC["Date"] = pd.to_datetime(MID_JUNDEC["Date"])
MID_JUNDEC = MID_JUNDEC.drop(columns={"InfoCodeSource", "SecCode", "SecCodeRegion", "SecCodeSource", "vf", "vt", "SecId","Sedol6", "Isin", "Ric"})
MID_JUNDEC = MID_JUNDEC.dropna(subset="InfoCode")

# Add 2024 MID Cap Universe
MID_2024 = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\SWEMCGV_2024.csv", parse_dates=["Date"], index_col=0)
MID_2024 = sqldf("""
                     SELECT * FROM MID_2024 AS Input
                     LEFT JOIN InfoCode AS Info
                     ON Info.StoxxId = Input.Internal_Number
                     WHERE Input.Date >= Info.vf
                     AND Input.Date <= Info.vt                     
                    """
                    )

MID_2024["Date"] = pd.to_datetime(MID_2024["Date"])
MID_2024 = MID_2024.drop(columns={"InfoCodeSource", "SecCode", "SecCodeRegion", "SecCodeSource", "vf", "vt", "SecId","Sedol6", "Isin", "Ric"})
MID_2024 = MID_2024.dropna(subset="InfoCode")

# Partial Concat
MID_JUNDEC = pd.concat([MID_JUNDEC, MID_2024])

# ================================================
#              Open -  Close Price
# ================================================
if Price == "Close":
    # Read CSV file for Cutoff dates (Market Cap)
    Securities_Cutoff_MARSEP = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Security_Cutoff\Output_Securities_Cutoff_MARSEP_NEW.csv",
                                        sep=",", parse_dates=["validDate"], index_col=0).rename(columns={"stoxxId": "stoxxId_Cutoff", "currency": "currency_Cutoff",
                                                                                                            "closePrice": "closePrice_Cutoff", "freeFloat": "freeFloat_Cutoff",
                                                                                                            "shares": "shares_Cutoff", "Capfactor": "Capfactor_Cutoff"})
                                        
    Securities_Cutoff_MARSEP = Securities_Cutoff_MARSEP.dropna(subset=["stoxxId_Cutoff"])

    # Add 2024 MARSEP Cutoff Information
    Securities_Cutoff_MARSEP_2024 = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Security_Cutoff\Output_Securities_Cutoff_MARSEP_NEW_2024.csv",
                                        sep=",", parse_dates=["validDate"], index_col=0).rename(columns={"stoxxId": "stoxxId_Cutoff", "currency": "currency_Cutoff",
                                                                                                            "closePrice": "closePrice_Cutoff", "freeFloat": "freeFloat_Cutoff",
                                                                                                            "shares": "shares_Cutoff", "Capfactor": "Capfactor_Cutoff"})
                                        
    Securities_Cutoff_MARSEP_2024 = Securities_Cutoff_MARSEP_2024.dropna(subset=["stoxxId_Cutoff"])

    Securities_Cutoff_MARSEP = pd.concat([Securities_Cutoff_MARSEP, Securities_Cutoff_MARSEP_2024])
                          
    Securities_Cutoff_JUNDEC = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Security_Cutoff\Output_Securities_Cutoff_JUNDEC_NEW.csv",
                                        sep=",", parse_dates=["validDate"], index_col=0).rename(columns={"stoxxId": "stoxxId_Cutoff", "currency": "currency_Cutoff",
                                                                                                            "closePrice": "closePrice_Cutoff", "freeFloat": "freeFloat_Cutoff",
                                                                                                            "shares": "shares_Cutoff", "Capfactor": "Capfactor_Cutoff"})

    Securities_Cutoff_JUNDEC = Securities_Cutoff_JUNDEC.dropna(subset=["stoxxId_Cutoff"])

    # Add 2024 JUNDEC Cutoff Information
    Securities_Cutoff_JUNDEC_2024 = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Security_Cutoff\Output_Securities_Cutoff_JUNDEC_NEW_2024.csv",
                                        sep=",", parse_dates=["validDate"], index_col=0).rename(columns={"stoxxId": "stoxxId_Cutoff", "currency": "currency_Cutoff",
                                                                                                            "closePrice": "closePrice_Cutoff", "freeFloat": "freeFloat_Cutoff",
                                                                                                            "shares": "shares_Cutoff", "Capfactor": "Capfactor_Cutoff"})

    Securities_Cutoff_JUNDEC_2024 = Securities_Cutoff_JUNDEC_2024.dropna(subset=["stoxxId_Cutoff"])

    Securities_Cutoff_JUNDEC = pd.concat([Securities_Cutoff_JUNDEC, Securities_Cutoff_JUNDEC_2024])

    # Concat the two Frames
    Securities_Cutoff = pd.concat([Securities_Cutoff_MARSEP, Securities_Cutoff_JUNDEC])

else:
    # Read CSV file for Cutoff dates (Market Cap)
    Securities_Cutoff_MARSEP = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Security_Cutoff\Output_Securities_Cutoff_MARSEP_NEW_OPEN.csv",
                                        sep=",", parse_dates=["validDate"], index_col=0).rename(columns={"stoxxId": "stoxxId_Cutoff", "currency": "currency_Cutoff",
                                                                                                            "adjustedOpenPrice": "openPrice_Cutoff", "freeFloat": "freeFloat_Cutoff",
                                                                                                            "shares": "shares_Cutoff", "Capfactor": "Capfactor_Cutoff"})
                                        
    Securities_Cutoff_MARSEP = Securities_Cutoff_MARSEP.dropna(subset=["stoxxId_Cutoff"])

    Securities_Cutoff_MARSEP_2024 = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Security_Cutoff\Output_Securities_Cutoff_MARSEP_NEW_2024_OPEN.csv",
                                        sep=",", parse_dates=["validDate"], index_col=0).rename(columns={"stoxxId": "stoxxId_Cutoff", "currency": "currency_Cutoff",
                                                                                                            "closePrice": "closePrice_Cutoff", "freeFloat": "freeFloat_Cutoff",
                                                                                                            "shares": "shares_Cutoff", "Capfactor": "Capfactor_Cutoff"})
                                        
    Securities_Cutoff_MARSEP_2024 = Securities_Cutoff_MARSEP_2024.dropna(subset=["stoxxId_Cutoff"])

    Securities_Cutoff_MARSEP = pd.concat([Securities_Cutoff_MARSEP, Securities_Cutoff_MARSEP_2024])
                   
    Securities_Cutoff_JUNDEC = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Security_Cutoff\Output_Securities_Cutoff_JUNDEC_NEW_OPEN.csv",
                                        sep=",", parse_dates=["validDate"], index_col=0).rename(columns={"stoxxId": "stoxxId_Cutoff", "currency": "currency_Cutoff",
                                                                                                            "adjustedOpenPrice": "openPrice_Cutoff", "freeFloat": "freeFloat_Cutoff",
                                                                                                            "shares": "shares_Cutoff", "Capfactor": "Capfactor_Cutoff"})

    Securities_Cutoff_JUNDEC = Securities_Cutoff_JUNDEC.dropna(subset=["stoxxId_Cutoff"])

    # Add 2024 JUNDEC Cutoff Information
    Securities_Cutoff_JUNDEC_2024 = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Security_Cutoff\Output_Securities_Cutoff_JUNDEC_NEW_2024_OPEN.csv",
                                        sep=",", parse_dates=["validDate"], index_col=0).rename(columns={"stoxxId": "stoxxId_Cutoff", "currency": "currency_Cutoff",
                                                                                                            "closePrice": "closePrice_Cutoff", "freeFloat": "freeFloat_Cutoff",
                                                                                                            "shares": "shares_Cutoff", "Capfactor": "Capfactor_Cutoff"})

    Securities_Cutoff_JUNDEC_2024 = Securities_Cutoff_JUNDEC_2024.dropna(subset=["stoxxId_Cutoff"])

    Securities_Cutoff_JUNDEC = pd.concat([Securities_Cutoff_JUNDEC, Securities_Cutoff_JUNDEC_2024])

    # Concat the two Frames
    Securities_Cutoff = pd.concat([Securities_Cutoff_MARSEP, Securities_Cutoff_JUNDEC])

# ================================================
# ================================================

# Read CSV files for Turnover
Turnover = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Turnover\Output_Turnover_Cutoff_3M_MARSEP.csv", 
                    parse_dates=["Date"], dtype={"InfoCode": "int64"})
Turnover_2024 = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Turnover\Output_Turnover_Cutoff_3M_MARSEP_2024.csv", 
                    parse_dates=["Date"], dtype={"InfoCode": "int64"})
Turnover = pd.concat([Turnover, Turnover_2024])

Turnover_JUNDEC = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Turnover\Output_Turnover_Cutoff_3M_JUNDEC.csv",
                    parse_dates=["Date"], dtype={"InfoCode": "int64"})
Turnover_JUNDEC_2024 = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Turnover\Output_Turnover_Cutoff_3M_JUNDEC_2024.csv",
                    parse_dates=["Date"], dtype={"InfoCode": "int64"})
Turnover_JUNDEC = pd.concat([Turnover_JUNDEC, Turnover_JUNDEC_2024])

# Merget the two Frame with Turnoveru
Turnover = pd.concat([Turnover, Turnover_JUNDEC]).sort_values(by="Date")

# Define parameters for Turnover
New = 0.15
Old = 0.10
# Define parameter for Market Cap
Threshold = 0.99
Threshold_Korea = 0.65

# Parameter for FOR
Threshold_FOR = 0.15
FOR_Removed = pd.DataFrame()
Removed_Securities = pd.DataFrame()
Added_Securities = pd.DataFrame()

# Version
Version = "20_Cutoff_Mcap_Enhanced"   

Output = pd.DataFrame()
Filtered = pd.DataFrame()

# Add Turnover Information
MID_MARSEP = MID_MARSEP.merge(Turnover[["InfoCode", "Turnover_Ratio", "Date", "Start", "End"]], left_on=["InfoCode", "Date"], 
                            right_on=["InfoCode", "Date"], how="left")

# Add Turnover Information
MID_JUNDEC = MID_JUNDEC.merge(Turnover[["InfoCode", "Turnover_Ratio", "Date", "Start", "End"]], left_on=["InfoCode", "Date"], 
                            right_on=["InfoCode", "Date"], how="left")

# Merge the two Frames with Input
Input = pd.concat([MID_MARSEP, MID_JUNDEC], ignore_index=True).sort_values(by="Date", ascending=True)

# Add Cutoff Date
Input = Input.merge(Dates_Frame, left_on="Date", right_on="Review")

# Set Turnover equal to 0 where NaN
Input["Turnover_Ratio"] = Input["Turnover_Ratio"].fillna(0)

# Add FX_Rate as of Cutoff
if FX_Rate_T_1 == False:
    FX_Rate = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\FX\FX_Historical_UPDATE.csv", index_col=0, parse_dates=["cutoff_date"])
    FX_Rate_2024 = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\FX\FX_Historical_UPDATE_2024.csv", index_col=0, parse_dates=["cutoff_date"])
    FX_Rate = pd.concat([FX_Rate, FX_Rate_2024])
    Input = Input.merge(FX_Rate, left_on=["Cutoff", "Currency"], right_on=["cutoff_date", "frm_currency"]).drop(columns={"frm_currency", 
                                "to_currency", "cutoff_date"}).rename(columns={"exchange_rate": "FX_Rate_Cutoff"})
else:
    FX_Rate = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\FX\FX_Historical_T-1_MID.csv", index_col=0, parse_dates=["cutoff_date"])
    Input = Input.merge(FX_Rate, left_on=["Cutoff", "Currency"], right_on=["cutoff_date", "frm_currency"]).drop(columns={"frm_currency", 
                                "to_currency", "cutoff_date"}).rename(columns={"exchange_rate": "FX_Rate_Cutoff"})

# Add information as of Cutoff
Input = Input.merge(Securities_Cutoff, left_on=["Cutoff", "Internal_Number"], 
        right_on=["validDate", "stoxxId_Cutoff"], how="left").drop(columns={"validDate", "stoxxId_Cutoff", "currency_Cutoff", "Capfactor_Cutoff", "Start", "End"})

# Calculate Full Market Cap
Input["Full_Market_Cap_Review"] = Input["Mcap_Units_Index_Currency"] / Input["Free_Float"]

# ================================================
#              Open -  Close Price
# ================================================
if Price == "Close":
    # Calculate USD Price at Cutoff
    Input["Close_USD_Cutoff"] = Input["closePrice_Cutoff"] * Input["FX_Rate_Cutoff"]

    # Calculate Free Float Market Cap as of Cutoff
    Input["Free_Float_Market_Cutoff"] = Input["Close_USD_Cutoff"] * Input["shares_Cutoff"] * Input["Free_Float"] * Input["Capfactor"]

    # Calculate Full Market Cap as of Cutoff
    Input["Full_Market_Cap_Cutoff"] = Input["Free_Float_Market_Cutoff"] / Input["Free_Float"] / Input["Capfactor"]

else:
    # Calculate USD Price at Cutoff
    Input["Open_USD_Cutoff"] = Input["openPrice_Cutoff"] * Input["FX_Rate_Cutoff"]

    # Calculate Free Float Market Cap as of Cutoff
    Input["Free_Float_Market_Cutoff"] = Input["Open_USD_Cutoff"] * Input["shares_Cutoff"] * Input["Free_Float"] * Input["Capfactor"]

    # Calculate Full Market Cap as of Cutoff
    Input["Full_Market_Cap_Cutoff"] = Input["Free_Float_Market_Cutoff"] / Input["Free_Float"] / Input["Capfactor"]

# ================================================
# ================================================

# Foreign Ownership Restriction adjusted Free Float Market Cap
Input["FOR_FF"] = Input["Free_Float"] * Input["Capfactor"]

# Filter for Date MAR2019
Input = Input.query("Date >= '2019-03-18'")
Dates_Frame = Dates_Frame.query("Review >= '2019-03-18'")

# Loop MID Cap
for date in Dates_Frame.Review:
    temp_Input = Input.query("Date == @date")

    # Pick the smallest 35% for each Country
    for country in temp_Input.Country.unique():
        country_Input = temp_Input.query("Country == @country")
        country_Input["Weight"] = country_Input["Full_Market_Cap_Cutoff"] / country_Input["Full_Market_Cap_Cutoff"].sum()
        country_Input = country_Input.sort_values(by="Weight", ascending=False)
        country_Input["CumulativeWeightCutoff"] = country_Input["Weight"].cumsum()

        country_Input = country_Input.query("CumulativeWeightCutoff >= (1 - 0.35)")

        Output = pd.concat([Output, country_Input])

# Add Source field
Output["Source"] = "MID Cap Index"

# Load initial setup V20
Input_V20 = pd.read_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\{Base}_Base\Final_Buffer_V20_Cutoff_Mcap_Enhanced_2024.csv", parse_dates=["Date"], index_col=0)
Input_V20 = Input_V20.query("Date >= '2019-03-18'")
Input_V20["Source"] = "SMALL Cap Index"

for date in Input_V20.Date.unique():
    temp_Output = Output.query("Date == @date")

    # ================================================
    #              Open -  Close Price
    # ================================================
    if Price == "Close":
        temp_Output = temp_Output[[
                                    "Date",
                                    "Index_Component_Count",
                                    "Internal_Number",
                                    "ISIN",
                                    "Instrument_Name",
                                    "Country",
                                    "Currency",
                                    "ICB",
                                    "Capfactor",
                                    "Free_Float",
                                    "Mcap_Units_Index_Currency",
                                    "InfoCode",
                                    "Free_Float_Market_Cutoff",
                                    "Full_Market_Cap_Cutoff",
                                    "FOR_FF",
                                    "Source"
                                ]]
    
        temp_Input_V20 = Input_V20.query("Date == @date")
        temp_Input_V20 = temp_Input_V20[[
                                    "Date",
                                    "Index_Component_Count",
                                    "Internal_Number",
                                    "ISIN",
                                    "Instrument_Name",
                                    "Country",
                                    "Currency",
                                    "ICB",
                                    "Capfactor",
                                    "Free_Float",
                                    "Mcap_Units_Index_Currency",
                                    "InfoCode",
                                    "Free_Float_Market_Cutoff",
                                    "Full_Market_Cap_Cutoff",
                                    "FOR_FF",
                                    "Source"
                                ]]
    
    else:
        temp_Output = temp_Output[[
                            "Date",
                            "Index_Component_Count",
                            "Internal_Number",
                            "ISIN",
                            "Instrument_Name",
                            "Country",
                            "Currency",
                            "ICB",
                            "Capfactor",
                            "Free_Float",
                            "Mcap_Units_Index_Currency",
                            "InfoCode",
                            "Free_Float_Market_Cutoff",
                            "Full_Market_Cap_Cutoff",
                            "FOR_FF",
                            "Source"
                        ]]
    
        temp_Input_V20 = Input_V20.query("Date == @date")
        temp_Input_V20 = temp_Input_V20[[
                                    "Date",
                                    "Index_Component_Count",
                                    "Internal_Number",
                                    "ISIN",
                                    "Instrument_Name",
                                    "Country",
                                    "Currency",
                                    "ICB",
                                    "Capfactor",
                                    "Free_Float",
                                    "Mcap_Units_Index_Currency",
                                    "InfoCode",
                                    "Free_Float_Market_Cutoff",
                                    "Full_Market_Cap_Cutoff",
                                    "FOR_FF",
                                    "Source"
                                ]]
        
    # ================================================
    # ================================================
    
    # Recalculate the Weight
    temp_Input_V20["Weight"] = temp_Input_V20["Full_Market_Cap_Cutoff"] / temp_Input_V20["Full_Market_Cap_Cutoff"].sum() 

    # Drop as many rows as temp_Output
    temp_Input_V20 = temp_Input_V20.sort_values(by="Weight", ascending=False)
    temp_Removed_Securities = temp_Input_V20.tail(len(temp_Output))
    temp_Input_V20 = temp_Input_V20.head(len(temp_Input_V20) - len(temp_Output))

    temp_Final_Frame = pd.concat([temp_Input_V20, temp_Output])

    # Add MCAP_UNITS_OPEN from SW_ACALLCAP
    temp_Final_Frame = temp_Final_Frame.merge(SWACALLCAP, on=["Date", "Internal_Number"], how="left")

    # Check if any securities do not have information
    if temp_Final_Frame['Mcap_Units_Index_Currency_Open'].isnull().any():
        print("There are empty (NaN) values in the column 'Mcap_Units_Index_Currency_Open'")

    if Price == "Open":
        temp_Final_Frame["Weight"] = temp_Final_Frame["Mcap_Units_Index_Currency_Open"] / temp_Final_Frame["Mcap_Units_Index_Currency_Open"].sum()
    else:
        temp_Final_Frame["Weight"] = temp_Final_Frame["Mcap_Units_Index_Currency"] / temp_Final_Frame["Mcap_Units_Index_Currency"].sum()
        temp_Final_Frame = temp_Final_Frame.drop(columns={"Mcap_Units_Index_Currency_Open"})

    temp_Final_Frame["Index_Component_Count"] = len(temp_Final_Frame)

    Final_Frame = pd.concat([Final_Frame, temp_Final_Frame])
    Removed_Securities = pd.concat([Removed_Securities, temp_Removed_Securities])
    Added_Securities = pd.concat([Added_Securities, temp_Output])

Final_Frame = Final_Frame.merge(SEDOLs, on=["Date", "Internal_Number"], how="left")

Final_Frame.drop(columns={"InfoCode", "Free_Float_Market_Cutoff", "Full_Market_Cap_Cutoff", "FOR_FF", "Source"}).to_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\{Price}\2024\{Base}\Final_Buffer_V20_Cutoff_Mcap_Enhanced_STEP2_2024_{Price}_{Base}.csv")
Final_Frame.drop(columns={"InfoCode", "Free_Float_Market_Cutoff", "Full_Market_Cap_Cutoff", "FOR_FF", "Source"}).query("Date == '2023-12-18'").to_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\{Price}\2024\{Base}\Final_Buffer_V20_Cutoff_Mcap_Enhanced_STEP2_2023_DEC_{Price}_{Base}.csv")
Removed_Securities.drop(columns={"Index_Component_Count", "InfoCode", "Free_Float_Market_Cutoff", "Full_Market_Cap_Cutoff", "FOR_FF", "Source"}).query("Date == '2023-12-18'").to_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\{Price}\2024\{Base}\Removed_Securities_STEP2_2023_DEC_{Price}_{Base}.csv")
Removed_Securities.drop(columns={"Index_Component_Count", "InfoCode", "Free_Float_Market_Cutoff", "Full_Market_Cap_Cutoff", "FOR_FF", "Source"}).to_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\{Price}\2024\{Base}\Removed_Securities_STEP2_FULL_{Price}_{Base}.csv")
Added_Securities.drop(columns={"InfoCode", "Free_Float_Market_Cutoff", "Full_Market_Cap_Cutoff", "FOR_FF", "Source"}).query("Date == '2023-12-18'").to_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\{Price}\2024\{Base}\Added_Securities_2023_DEC_{Price}_{Base}.csv")

# Read CapFactor from SWACALLCAP
Capfactor = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\Capfactor_SWACALLCAP_MAR2019.csv", index_col=0, parse_dates=["Date"])

# Drop JUN2024 composition
Final_Frame = Final_Frame.query("Date < '2024-06-24'").drop(columns={"Capfactor", "Mcap_Units_Index_Currency"})
Final_Frame = Final_Frame.merge(Capfactor, on=["Date", "Internal_Number"], how="left")

Sharable_Frame = pd.DataFrame()

# Recalculate Weights
for date in Final_Frame.Date.unique():
    temp_Final_Frame = Final_Frame.query("Date == @date")

    temp_Final_Frame["Weight"] = temp_Final_Frame["Mcap_Units_Index_Currency"] / temp_Final_Frame["Mcap_Units_Index_Currency"].sum()

    Sharable_Frame = pd.concat([temp_Final_Frame, Sharable_Frame])

Sharable_Frame.sort_values(by="Date", ascending=True).drop(columns={"InfoCode", "Free_Float_Market_Cutoff", "Full_Market_Cap_Cutoff", "FOR_FF", "Source"}).to_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Output\{Price}\2024\{Base}\Sharable_Final_Buffer_V20_Cutoff_Mcap_Enhanced_STEP2_{Price}_{Base}.csv")