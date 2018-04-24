import datetime
import win32com.client
import time
import pandas as pd
import matplotlib.pyplot as plt

import pandas.io.sql as pd_sql
import numpy as np
import sqlalchemy as sa
import pymysql
pymysql.install_as_MySQLdb()
import datetime as datetime

conn = sa.create_engine("mysql://root:Betfair@localhost/betfairdb?host=localhost?port=3306",convert_unicode=True)

# Read Sql
def read_sql(table):
    df = pd.read_sql(table, conn)
    return df

while True:
    current_time = str(datetime.datetime.now().strftime('%Y%m%d_%H%M'))
    directory_png = 'E:\Mail_Alerts\AlertsTable_' + current_time + '.png'
    directory_xlsx = 'E:\Mail_Alerts\Summary_' + current_time + '.xlsx'
    print(directory_png)
    print(directory_xlsx)

    table = "strategies"
    df = read_sql(table)
    df.to_excel(directory_xlsx)

    df = df[df['status']=='active']

    # Generate .png out of df
    from pandas.plotting import table
    fig, ax = plt.subplots(figsize=(20, 10))
    ax.axis('off')
    tabla = table(ax, df, loc='center')
    tabla.auto_set_font_size(False)
    tabla.set_fontsize(12)
    tabla.scale(1.1, 2.5)
    plt.savefig(directory_png)

    # Send .xlsx as attachment and .png as body
    # recipient = "flv.port@gmail.com; milan.thomas.ad@gmail.com; ahmed.trablsi@gmail.com"
    recipient = "milan.thomas.ad@gmail.com"
    subject = "Betfair activity - " + current_time

    olMailItem = 0x0
    obj = win32com.client.Dispatch("Outlook.Application")
    newMail = obj.CreateItem(olMailItem)
    newMail.Subject = subject

    newMail.Attachments.Add(directory_xlsx)

    newMail.HTMLBody = '<img src= ' + directory_png + ' ></img>'

    newMail.To = recipient

    newMail.Display()

    newMail.Send()

    time.sleep(6*60*60)

