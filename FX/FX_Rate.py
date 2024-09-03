import pandas as pd
import requests
import datetime
from datetime import datetime, timedelta, date

MARSEP = pd.read_csv(r"C:\Users\et246\Desktop\stoxx-world-msci\V18\Dates\Review_Date-MAR-SEP.csv", index_col=0, parse_dates=["Review", "Cutoff"])
JUNDEC = pd.read_csv(r"C:\Users\et246\Desktop\stoxx-world-msci\V18\Dates\Review_Date-JUN-DEC.csv", index_col=0, parse_dates=["Review", "Cutoff"])

Dates = pd.concat([MARSEP, JUNDEC]).sort_values(by=["Review"])
Currency = ["KRW", "MYR", "HKD", "TWD", "ILS", "EUR", "PHP", "THB", "USD", "CLP", "MXN", "ZAR", "EGP", 
            "HUF", "TRY", "IDR", "CNY", "INR", "ARS", "BRL", "PLN", "GRD", "CZK", "MAD", "AUD", "RUB", "COP", "SGD", 
            "JOD", "JPY", "GBP", "PKR", "SAR", "KWD", "QAR", "AED"]

TargetCCY = "USD"
Historical_FX = pd.DataFrame()

for Cutoff in Dates["Cutoff"]:
    
    Cutoff = Cutoff.date()

    for FX in Currency:
        URL = "http://maddox2.prod.ci.dom/sidwebapi/api/Currency/GetCurrencyRateRange?frmCcys={}&toCcys={}&startDate={}&endDate={}".format(FX, TargetCCY, Cutoff, Cutoff)

        # Make an HTTP GET request to the URL and get the response
        response = requests.get(url=URL)

        # Check if the request was successful
        if response.status_code == 200:
            content = response.json()

            # Store the FX into a Frame
            FX = pd.DataFrame(content).drop(columns={"bid_rate", "ask_rate", "status"})

            Historical_FX = pd.concat([Historical_FX, FX])

print(Historical_FX)
Historical_FX = Historical_FX.to_csv(r"C:\Users\et246\Desktop\stoxx-world-msci\V18\FX\FX_Historical_UPDATE.csv")