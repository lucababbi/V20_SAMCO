from sqlite3 import Date
import sys
sys.path.append(r"C:\Users\et246\Desktop\V20_SAMCO\STOXX")
import pandas as pd
from datetime import datetime
from stoxx.qad.Turnover_Code import get_turnover_ratio
from stoxx.qad.identifier import get_infocode

Output_Turnover = pd.DataFrame()

InfoCode = pd.read_csv(r"C:\Users\et246\Desktop\V20_SAMCO\InfoCode.csv", parse_dates=["vt"])
# Deal with 99991230 dates with a date in remote future
InfoCode["vt"] = InfoCode["vt"].replace("99991230", "21001230")
# Convert columns into DateTime
InfoCode["vt"] = pd.to_datetime(InfoCode["vt"], format = "%Y%m%d")
Infocode = InfoCode.loc[InfoCode.groupby("StoxxId")["vt"].idxmax()]

# Get the Universe at Review Dates
Output_JUNDEC = pd.read_csv(r"C:\Users\et246\Desktop\V20_SAMCO\Universe\SWESCGV_JUNDEC_2024.csv", index_col=0, parse_dates=["Date", "Cutoff"])
Output_MARSEP = pd.read_csv(r"C:\Users\et246\Desktop\V20_SAMCO\Universe\SWESCGV_MARSEP_2024.csv", index_col=0, parse_dates=["Date", "Cutoff"])

Output_JUNDEC = Output_JUNDEC.merge(Infocode[["StoxxId", "InfoCode"]], left_on="Internal_Number", right_on="StoxxId", how="left").drop(columns={"StoxxId"})
Output_MARSEP = Output_MARSEP.merge(Infocode[["StoxxId", "InfoCode"]], left_on="Internal_Number", right_on="StoxxId", how="left").drop(columns={"StoxxId"})

# Get Dates Frame
Dates_FrameJUNDEC = pd.read_csv(r"C:\Users\et246\Desktop\V20_SAMCO\Dates\Review_Date-JUN-DEC.csv", index_col=0, 
                        parse_dates=["Cutoff", "Review"]).tail(1)
Dates_FrameMARSEP = pd.read_csv(r"C:\Users\et246\Desktop\V20_SAMCO\Dates\Review_Date-MAR-SEP.csv", index_col=0, 
                        parse_dates=["Cutoff", "Review"]).tail(1)

# Drop SEDOL
Output_JUNDEC = Output_JUNDEC.dropna(subset=["SEDOL", "InfoCode"])
Output_MARSEP = Output_MARSEP.dropna(subset=["SEDOL", "InfoCode"])

def last_business_day(date):
    # Convert date to pandas Timestamp
    date = pd.Timestamp(date)
    
    # Move the date to the last business day of the month
    last_day_of_month = pd.Timestamp(date.year, date.month - 1, 1) + pd.offsets.BMonthEnd()
    
    # Move backwards until we find a business day
    while True:
        if last_day_of_month.dayofweek < 5:  # Monday is 0 and Sunday is 6
            return last_day_of_month
        else:
            last_day_of_month -= pd.Timedelta(days=1)

def first_business_day(date):
    # Convert date to pandas Timestamp
    date = pd.Timestamp(date)
    
    # Move the date to the last business day of the month
    last_day_of_month = pd.Timestamp(date.year, date.month, 1)
    
    # Move backwards until we find a business day
    while True:
        if last_day_of_month.dayofweek < 5:  # Monday is 0 and Sunday is 6
            return last_day_of_month
        else:
            last_day_of_month += pd.Timedelta(days=1)

# Turnover for MAR/SEP
for date in Dates_FrameMARSEP["Review"]:
    enddate = last_business_day(date)
    startdate = first_business_day(enddate - pd.DateOffset(months = 2))
    temp_Output = Output_MARSEP.query("Date == @date")
    AA = get_turnover_ratio(temp_Output["SEDOL"].tolist(), temp_Output["InfoCode"].tolist(), startdate, enddate, sedoldate = None)
    AA["Date"] = date
    AA["Start"] = startdate
    AA["End"] = enddate
    Output_Turnover = pd.concat([Output_Turnover, AA])
    print(date)

Output_Turnover.to_csv(r"C:\Users\et246\Desktop\V20_SAMCO\Turnover\Output_Turnover_Cutoff_3M_MARSEP_2024.csv")

# Reset the DataFrame
Output_Turnover = pd.DataFrame()

# Turnover for JUN/DEC
for date in Dates_FrameJUNDEC["Review"]:
    enddate = last_business_day(date)
    startdate = first_business_day(enddate - pd.DateOffset(months = 2))
    temp_Output = Output_JUNDEC.query("Date == @date")
    AA = get_turnover_ratio(temp_Output["SEDOL"].tolist(), temp_Output["InfoCode"].tolist(), startdate, enddate, sedoldate = None)
    AA["Date"] = date
    AA["Start"] = startdate
    AA["End"] = enddate
    Output_Turnover = pd.concat([Output_Turnover, AA])
    print(date)

Output_Turnover.to_csv(r"C:\Users\et246\Desktop\V20_SAMCO\Turnover\Output_Turnover_Cutoff_3M_JUNDEC_2024.csv")
