import pandas as pd

# Read Base V24
MAR = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Recapper\Input\V24_18MAR2021.csv", 
                  parse_dates=["closeDay"], sep=";")[["closeDay", "internalNumber"]]

SEP = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Recapper\Input\V24_18JUN2021.csv",
                  parse_dates=["closeDay"], sep=";")[["closeDay", "internalNumber"]]

# Read STXWAGV MCAP
SW = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Recapper\Input\STXWAGV_MAR-JUN-2021.csv",
                 index_col=0, parse_dates=["Date"], sep=";")

# Add info MCAP
MAR = MAR.merge(SW, left_on=["closeDay", "internalNumber"], right_on=["Date", "Internal_Number"], how="left").drop(columns={"internalNumber"})
SEP = SEP.merge(SW, left_on=["closeDay", "internalNumber"], right_on=["Date", "Internal_Number"], how="left").drop(columns={"internalNumber"})

S1 = MAR["Mcap_Units_Index_Currency"].sum()
S2 = SEP["Mcap_Units_Index_Currency"].sum()

Return_V24 = S2 / S1 - 1
print(Return_V24)

