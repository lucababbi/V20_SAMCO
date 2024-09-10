import polars as pl
import pandas as pd

# Load Parquet
ToR = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Turnover\Turnover_SID.parquet")
# Drop unuseful columns
ToR = ToR.drop(["vd", "calcType", "token"])
# Keep only relevant fields
ToR = ToR.filter(pl.col("field").is_in(["TurnoverRatioFO"]))

# Transform the table
ToR = ToR.pivot(
                values="Turnover_Ratio",
                index=["Date", "Internal_Number"],
                on="field"
                )

# Load Universe SW Emerging Markets Small Cap
# Read CSV file, parse dates, handle NA values, drop rows with NA, and specify dtype
PRE = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\Updated_Universe\PRE_MAR_2023.csv", index_col=0)

# Remove empty/null Securities
PRE = PRE.query("Mcap_Units_Index_Currency > 0")

# Re-arrange PRE Frame to adapt to newer one
PRE = PRE[["Date", "Index_Symbol", "Internal_Number", "ISIN", "SEDOL", "RIC", "Instrument_Name", "Country", "Currency", "exchange", 
           "ICB", "Shares", "Free_Float", "Capfactor", "Close_unadjusted_local", "FX_local_to_Index_Currency", "Mcap_Units_Index_Currency", "Weight"]].rename(columns={"exchange": "Exchange"})
    
POST = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Universe\Updated_Universe\POST_MAR_2023.csv", index_col=0)

# Remove empty/null Securities
POST = POST.query("Mcap_Units_Index_Currency > 0")

# Remove unuseful columns
POST = POST[["Date", "Index_Symbol", "Internal_Number", "ISIN", "SEDOL", "RIC", "Instrument_Name", "Country", "Currency", "Exchange",
             "ICB", "Shares", "Free_Float", "Capfactor", "Close_unadjusted_local", "FX_local_to_Index_Currency", "Mcap_Units_Index_Currency", "Weight"]]

# Concat the two Inputs
Input = pd.concat([PRE, POST])

# Keep only needed columns
Input = pl.from_pandas(Input[["Date", "Internal_Number", "Weight"]])

# Add ToR information
Input = Input.join(ToR, on=["Date", "Internal_Number"], how="left")

# Final Output Frame
Output = pl.DataFrame()

# Create Recap file
for date in Input.select("Date").unique().sort("Date", descending=False).to_series():
    temp_Input = Input.filter(pl.col("Date") == date)

    # Create the Recap Frame
    temp_Output = pl.DataFrame({
                                "Date": date,
                                "No_Null_Size": len(temp_Input.filter(pl.col("TurnoverRatioFO").is_not_null())),
                                "Total": len(temp_Input),
                                "Count": len(temp_Input.filter(pl.col("TurnoverRatioFO").is_not_null())) / len(temp_Input),
                                "Weight": temp_Input.filter(pl.col("TurnoverRatioFO").is_not_null()).select(pl.col("Weight").sum())
    })

    Output = pl.concat([Output, temp_Output])

Output.write_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Turnover\SW_EM_SMALLCAP_ToR_Coverage.csv")