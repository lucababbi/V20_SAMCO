from logging import Formatter
from multiprocessing import connection
from sqlite3 import Cursor, connect
import sys
sys.path.append(r"C:\Users\et246\Desktop\V20_SAMCO\STOXX")
import pandas as pd
import numpy as np
import datetime as dt
import stoxx as stx
from stoxx.qad import con
from pandas.tseries.offsets import BDay
from stoxx.icb.icb import ICB
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import calendar
import warnings
warnings.filterwarnings("ignore")
import pyodbc
import sqlalchemy as sal
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import urllib

server = "brutus1.bat.ci.dom,1433"
database = "SIDDB"

# Construct connection string
connection_string = f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

# Create engine
engine = create_engine(connection_string)

try:
    # Test connection by executing a simple query
    with engine.connect() as connection:
        result = connection.execute("SELECT 1")
        print("Connection successful!")
except OperationalError as e:
    print("Connection failed:", e)

MARSEP = pd.read_csv(r"C:\Users\et246\Desktop\V20_SAMCO\Dates\Review_Date-MAR-SEP.csv", parse_dates=["Review", "Cutoff"]).tail(1)
JUNDEC = pd.read_csv(r"C:\Users\et246\Desktop\V20_SAMCO\Dates\Review_Date-JUN-DEC.csv", parse_dates=["Review", "Cutoff"]).tail(1)

Output = pd.DataFrame()
CapFactor = pd.DataFrame()

GetSecurity = """
        IF OBJECT_ID('tempdb..##stoxxId') IS NOT NULL
            DROP TABLE ##stoxxidList;

        CREATE TABLE ##stoxxidList (
            stoxxId VARCHAR(12) NULL
        );

        INSERT INTO ##stoxxidList (stoxxId)
        SELECT stoxxid FROM ( VALUES('')) a(stoxxid);

        DECLARE
            @startDateInput    INT          = :start_date,
            @endDateInput      INT          = :end_date,
            @calendarNameInput VARCHAR(200) = 'stoxxcal';

        /* ------ parameter sniffing ------ */


        DECLARE @familyId INT = (
            SELECT id
            FROM [sid].indexFamily
            WHERE name = 'STOXX'
        );

        IF OBJECT_ID('tempdb..##stoxxId') IS NOT NULL
            DROP TABLE ##stoxxId;

        CREATE TABLE ##stoxxId (
            securityId INT NOT NULL,
            stoxxId VARCHAR(12) NOT NULL,
            vd INT NOT NULL,
            prevVd INT NOT NULL,
            PRIMARY KEY CLUSTERED ([securityId], vd ASC)
            WITH (
                PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF,
                ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95
            ) ON [PRIMARY]
        );

        IF OBJECT_ID('tempdb..##calendarDates') IS NOT NULL
            DROP TABLE ##calendarDates;

        CREATE TABLE ##calendarDates (
            name VARCHAR(55) NOT NULL,
            vd INT NOT NULL,
            prevVd INT NOT NULL
        );

        IF OBJECT_ID('tempdb..##openPrice') IS NOT NULL
            DROP TABLE ##openPrice;

        IF OBJECT_ID('tempdb..##closePrice') IS NOT NULL
            DROP TABLE ##closePrice;

        IF OBJECT_ID('tempdb..##freefloat') IS NOT NULL
            DROP TABLE ##freefloat;

        INSERT INTO ##calendarDates (name, vd, prevVd)
        SELECT c.name, cd.vd, cd.prevVd
        FROM [sid].calendar c
        JOIN [sid].calendarDate cd ON cd.calendarId = c.id
        WHERE c.name = @calendarNameInput
        AND cd.vd >= @startDateInput
        AND cd.vd <= @endDateInput;

        CREATE NONCLUSTERED INDEX [IX_##calendarDates_vd] ON ##calendarDates (vd);
        CREATE NONCLUSTERED INDEX [IX_##calendarDates] ON ##calendarDates (vd, prevVd);

        INSERT INTO ##stoxxId (securityId, stoxxId, vd, prevVd)
        SELECT sd.securityId, s.stoxxId, cd.vd, cd.prevVd
        FROM sid.securityDescription sd
        JOIN ##calendarDates cd ON sd.vf <= cd.vd AND sd.vt > cd.vd
        JOIN sid.security s ON s.id = sd.securityId
        JOIN ##stoxxidList SL ON SL.stoxxId = s.stoxxId;

        DECLARE @count INT = (
            SELECT COUNT(1)
            FROM ##stoxxidList
            WHERE stoxxId <> ''
        );

        INSERT INTO ##stoxxId (securityId, stoxxId, vd, prevVd)
        SELECT sd.securityId, s.stoxxId, cd.vd, cd.prevVd
        FROM sid.securityDescription sd
        JOIN ##calendarDates cd ON sd.vf <= cd.vd AND sd.vt > cd.vd
        JOIN sid.security s ON s.id = sd.securityId
        WHERE @count < 1;

        SELECT o.securityId, o.openPr, o.openNr, o.openGr, o.currencyId, o.adjustmentFactor, o.vd, o.prevVd
        INTO ##openPrice
        FROM [sid].[open] AS o
        JOIN ##stoxxId AS c ON o.securityId = c.securityId AND o.vd = c.vd AND o.prevVd = c.prevVd
        OPTION (MAXDOP 1);

        SELECT j.securityId, j.close_, j.currencyId, j.vd
        INTO ##closePrice
        FROM [sid].[close] AS j
        JOIN ##stoxxId AS c ON j.securityId = c.securityId AND j.vd = c.vd;

        SELECT i.securityId, i.freefloatValue, c.vd
        INTO ##freefloat
        FROM [sid].freefloat AS i
        JOIN ##stoxxId AS c ON i.securityId = c.securityId AND c.vd >= i.vf AND c.vd < i.vt
        WHERE i.indexFamilyId = @familyId;

        SELECT
            s.vd AS validDate,
            s.stoxxId,
            cr.symbol AS currency,
            o.openPr AS adjustedOpenPrice,
            o.openNr AS adjustedOpenNet,
            o.openGr AS adjustedOpenGross,
            o.adjustmentFactor AS adjustmentFactor,
            j.close_ AS closePrice,
            i.freefloatValue AS freeFloat,
            h.outNoShares AS shares
        FROM ##stoxxId s
        INNER JOIN [sid].share AS h ON h.securityId = s.securityId
        AND h.vf <= s.vd AND s.vd < h.vt
        AND h.indexFamilyId = @familyId
        INNER JOIN ##freefloat AS i ON i.securityId = s.securityId AND i.vd = s.vd
        LEFT JOIN ##closePrice AS j ON j.securityId = s.securityId AND j.vd = s.vd
        LEFT JOIN ##openPrice AS o ON o.securityId = s.securityId AND o.vd = s.vd AND o.prevVd = s.prevVd
        LEFT JOIN [sid].currency AS cr ON cr.id = o.currencyId;

"""

# Get Security data as of MARSEP Cutoff
for date in MARSEP["Cutoff"]:

    date = date.strftime('%Y%m%d')

    with engine.connect() as connection:
        # Execute the initial SQL script
        connection.execute(text(GetSecurity), start_date=date, end_date=date)
        # Define the SQL query with parameters
        sSQL = """
        SELECT
            s.vd AS validDate,
            s.stoxxId,
            cr.symbol AS currency,
            o.openPr AS adjustedOpenPrice,
            o.openNr AS adjustedOpenNet,
            o.openGr AS adjustedOpenGross,
            o.adjustmentFactor AS adjustmentFactor,
            j.close_ AS closePrice,
            i.freefloatValue AS freeFloat,
            h.outNoShares AS shares
        FROM ##stoxxId s
        INNER JOIN [sid].share AS h ON h.securityId = s.securityId
        AND h.vf <= s.vd AND s.vd < h.vt
        AND h.indexFamilyId = :familyId
        INNER JOIN ##freefloat AS i ON i.securityId = s.securityId AND i.vd = s.vd
        LEFT JOIN ##closePrice AS j ON j.securityId = s.securityId AND j.vd = s.vd
        LEFT JOIN ##openPrice AS o ON o.securityId = s.securityId AND o.vd = s.vd AND o.prevVd = s.prevVd
        LEFT JOIN [sid].currency AS cr ON cr.id = o.currencyId;
        """

        # Execute the query with parameters
        result = connection.execute(text(sSQL), familyId=1)

        # Fetch the results and convert to DataFrame
        temp = pd.DataFrame(result.fetchall())
        temp = temp.drop(columns={"adjustedOpenPrice", "adjustedOpenNet", "adjustedOpenGross", "adjustmentFactor"})

        # Add CapFactor information
        sSQL = """
                SELECT
                    i.symbol AS indexSymbol,
                    s.stoxxId,
                    c.Capfactor
                FROM
                    [sid].[component] AS c
                JOIN
                    [sid].[security] AS s ON c.securityId = s.id
                JOIN
                    [sid].[index] AS i ON c.indexId = i.id
                WHERE
                    c.vf <= :date_param
                    AND c.vt > :date_param
                    AND i.symbol = 'STXWAP'
                """

        # Execute the query with individual parameters
        result = connection.execute(sal.text(sSQL), date_param=date)

        # Fetch the results if needed
        rows = result.fetchall()

        # Convert the fetched rows to a DataFrame
        result = pd.DataFrame(rows, columns=['indexSymbol', 'stoxxId', 'Capfactor'])

        result["Date"] = date

        CapFactor = pd.concat([CapFactor, result])

        # Merge the Output with CapFactor
        temp = temp.merge(CapFactor[["stoxxId", "Capfactor"]], left_on=["stoxxId"], right_on=["stoxxId"], how="left")

        Output = pd.concat([Output, temp])

Output = Output.drop_duplicates(subset=["validDate", "stoxxId"])
Output.to_csv(r"C:\Users\et246\Desktop\V20_SAMCO\Security_Cutoff\Output_Securities_Cutoff_MARSEP_NEW_2024.csv")

# Reset the Frame
Output = pd.DataFrame()
CapFactor = pd.DataFrame()

# Get Security data as of JUNDEC Cutoff
for date in JUNDEC["Cutoff"]:

    date = date.strftime('%Y%m%d')

    with engine.connect() as connection:
        # Execute the initial SQL script
        connection.execute(text(GetSecurity), start_date=date, end_date=date)
        # Define the SQL query with parameters
        sSQL = """
        SELECT
            s.vd AS validDate,
            s.stoxxId,
            cr.symbol AS currency,
            o.openPr AS adjustedOpenPrice,
            o.openNr AS adjustedOpenNet,
            o.openGr AS adjustedOpenGross,
            o.adjustmentFactor AS adjustmentFactor,
            j.close_ AS closePrice,
            i.freefloatValue AS freeFloat,
            h.outNoShares AS shares
        FROM ##stoxxId s
        INNER JOIN [sid].share AS h ON h.securityId = s.securityId
        AND h.vf <= s.vd AND s.vd < h.vt
        AND h.indexFamilyId = :familyId
        INNER JOIN ##freefloat AS i ON i.securityId = s.securityId AND i.vd = s.vd
        LEFT JOIN ##closePrice AS j ON j.securityId = s.securityId AND j.vd = s.vd
        LEFT JOIN ##openPrice AS o ON o.securityId = s.securityId AND o.vd = s.vd AND o.prevVd = s.prevVd
        LEFT JOIN [sid].currency AS cr ON cr.id = o.currencyId;
        """

        # Execute the query with parameters
        result = connection.execute(text(sSQL), familyId=1)

        # Fetch the results and convert to DataFrame
        temp = pd.DataFrame(result.fetchall())
        temp = temp.drop(columns={"adjustedOpenPrice", "adjustedOpenNet", "adjustedOpenGross", "adjustmentFactor"})

        # Add CapFactor information
        sSQL = """
                SELECT
                    i.symbol AS indexSymbol,
                    s.stoxxId,
                    c.Capfactor
                FROM
                    [sid].[component] AS c
                JOIN
                    [sid].[security] AS s ON c.securityId = s.id
                JOIN
                    [sid].[index] AS i ON c.indexId = i.id
                WHERE
                    c.vf <= :date_param
                    AND c.vt > :date_param
                    AND i.symbol = 'STXWAP'
                """

        # Execute the query with individual parameters
        result = connection.execute(sal.text(sSQL), date_param=date)

        # Fetch the results if needed
        rows = result.fetchall()

        # Convert the fetched rows to a DataFrame
        result = pd.DataFrame(rows, columns=['indexSymbol', 'stoxxId', 'Capfactor'])

        result["Date"] = date

        CapFactor = pd.concat([CapFactor, result])

        # Merge the Output with CapFactor
        temp = temp.merge(CapFactor[["stoxxId", "Capfactor"]], left_on=["stoxxId"], right_on=["stoxxId"], how="left")

        Output = pd.concat([Output, temp])

Output = Output.drop_duplicates(subset=["validDate", "stoxxId"])
Output.to_csv(r"C:\Users\et246\Desktop\V20_SAMCO\Security_Cutoff\Output_Securities_Cutoff_JUNDEC_NEW_2024.csv")