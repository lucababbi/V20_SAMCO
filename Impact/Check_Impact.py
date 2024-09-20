import polars as pl
import pandasql
import pandas as pd

# Import Impacted Securities
Impacted_Securities = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Impact\Impacted_Securities.csv", separator=";", infer_schema=False).with_columns(
                                    pl.col("startDate").str.strptime(pl.Date, format="%Y%m%d").alias("startDate"),
                                    pl.col("endDate").str.strptime(pl.Date, format="%Y%m%d").alias("endDate")
                                 )

# NASDAQ
NASDAQ = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Impact\NASDAQ.csv", infer_schema=False, try_parse_dates=True).with_columns(
                                    pl.col("Date").str.strptime(pl.Date, format="%Y-%m-%d")
).select(pl.col(["Date", "Internal_Number"]))

# Join the two DataFrames
sSQL = """SELECT *
          FROM NASDAQ
          LEFT JOIN Impacted_Securities
          ON NASDAQ.Internal_Number = Impacted_Securities.stoxxid
          WHERE NASDAQ.Date >= Impacted_Securities.startDate
          AND NASDAQ.Date <= Impacted_Securities.endDate"""

# Convert the two Frames to Pandas
NASDAQ = NASDAQ.to_pandas()
Impacted_Securities = Impacted_Securities.to_pandas()

Final = pandasql.sqldf(sSQL).drop(columns={"stoxxid"}).to_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Impact\Output\NASDAQ_Output_Impacts.csv", index=False)

# Vietnam
Vietnam = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Impact\Vietnam.csv", infer_schema=False, separator=";").with_columns(
                                    pl.col("effectiveDate").str.strptime(pl.Date, format="%Y%m%d").alias("Date")
).select(pl.col(["Date", "STOXXID"])).rename({"STOXXID": "Internal_Number"}).to_pandas()

# Join the two DataFrames
sSQL = """SELECT *
          FROM Vietnam
          LEFT JOIN Impacted_Securities
          ON Vietnam.Internal_Number = Impacted_Securities.stoxxid
          WHERE Vietnam.Date >= Impacted_Securities.startDate
          AND Vietnam.Date <= Impacted_Securities.endDate"""

Final = pandasql.sqldf(sSQL).drop(columns={"stoxxid"}).to_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V20_SAMCO\Impact\Output\Vietnam_Output_Impacts.csv", index=False)
