import pandas as pd

Daily_Comp = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Analysis_V25\Daily_Composition_V25.csv", sep=";", parse_dates=["Date"], index_col=0)

Pivot = Daily_Comp.pivot(index="Internal_Number", columns="Date", values="Mcap_Units_Index_Currency")
Group_Pivot = Pivot.sum(axis=0).to_frame(name="Daily_Sum")
Group_Pivot.to_clipboard()