from requests import get
import pandas as pd
from urllib3 import exceptions, disable_warnings
import datetime
disable_warnings(exceptions.InsecureRequestWarning)

date_ranges =  pd.concat([pd.read_csv(r"C:\Users\et246\Desktop\V20_SAMCO\Dates\Review_Date-JUN-DEC.csv", index_col=0, parse_dates=["Review", "Cutoff"]), 
                                                        pd.read_csv(r"C:\Users\et246\Desktop\V20_SAMCO\Dates\Review_Date-MAR-SEP.csv", index_col=0, parse_dates=["Review", "Cutoff"])])
 
date_ranges = date_ranges.sort_values(by="Review")[:110].tail(22)
dates = date_ranges["Review"].tolist()


header = {
    "Content-Type": "application/json",
    "iStudio-User": "lbabbi@qontigo.com"
}
 
batch_ids = [18353]
 
batch_name = 'Output_file'

server_type = "PROD"
 
Output = pd.DataFrame()
 
# Script variables
 
server = {"PROD" : {
        "url" : "https://vmindexstudioprd01:8002/",
        "ssl" : False
    }
}
 
for batch_id in batch_ids:
    composition = pd.DataFrame()
    for current_date in dates :
        try:
            json_result = get(
                    url = "{}/api/2.0/analytics/batch/{}/composition/export/{}/".format(server[server_type]["url"], batch_id, current_date.strftime("%Y-%m-%d")),
                    headers = header, verify = server[server_type]["ssl"]).json()
            if "data" in json_result.keys() :
                print("Components found for batch id: "+str(batch_id)+ ", keyword: "+str(batch_id)+ " at date - {}".format(current_date))
                composition = composition.append(pd.json_normalize(json_result["data"]["composition_export"]))
                composition = composition[(composition['index_type'] == 'Gross Return') & (composition['index_currency'] == 'USD')]

            print(str(current_date) + " done!")
        except:
            next

# Rename columns to merge with Output from Live Index
composition = composition.rename(columns={"close_day": "Date","index_symbol": "Index_Symbol", "index_name": "Index_Name",
                                        "index_type": "Index_Type", "index_currency": "Index_Currency", "index_close": "Index_Close",
                                        "index_component_count": "Index_Component_Count", "index_mcap_units": "Index_Mcap_Units",
                                        "index_divisor": "Index_Divisor", "internal_number": "Internal_Number", "isin":	"ISIN", "sedol": "SEDOL",
                                        "ric": "RIC", "cusip": "CUSIP", "name": "Instrument_Name", "country": "Country", "currency": "Currency",
                                        "icb5": "ICB", "shares_close": "Shares", "free_float_close": "Free_Float", "cap_factor_cf": "Capfactor",
                                        "weight_factor_wf": "Weightfactor", "close_unadjusted_local": "Close_unadjusted_local",	
                                        "fx_local_to_index_currency_close": "FX_local_to_Index_Currency", "mcap_units_local": "Mcap_Units_local",
                                        "mcap_units_index_currency": "Mcap_Units_Index_Currency", "weight": "Weight"
})

# Remove unuseful columns
composition = composition[["Date", "Country", "Weight"]]

Output = pd.DataFrame(columns=[
                                "Date",
                                "IN",
                                "CN"
                              ])

for date in composition.Date.unique():
    temp_composition = composition.query("Date == @date")

    temp_Output = pd.DataFrame({
                                "Date": [date],
                                "IN": [temp_composition.query("Country == 'IN'")["Weight"].sum()],
                                "CN": [temp_composition.query("Country == 'CN'")["Weight"].sum()]
                               })

    Output = pd.concat([temp_Output, Output])

Output.sort_values(by="Date").to_csv(r"C:\Users\et246\Desktop\V20_SAMCO\Output\Recap\CN_IN_Recap_V24_18353.csv")