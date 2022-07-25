import mysql.connector
import pandas as pd
import numpy as np
import csv
import smtplib
from email.message import EmailMessage

DataBase = mysql.connector.connect(
    host="localhost",
    user="root",
    password="mynewpassword",
    autocommit=True
)


SENDER_EMAIL = "jainsansi@gmail.com"
APP_PASSWORD = "zqmblhdhmharxijz"

def mail(recipient_email, subject, content, file):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = recipient_email
    msg.set_content(content)

    with open(file, 'rb') as f:
        file_data = f.read()
    msg.add_attachment(file_data, maintype="application", subtype="csv", filename=file)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(SENDER_EMAIL, APP_PASSWORD)
        smtp.send_message(msg)


def check_d(column_name,table_schema,table_name):
    Cursor = DataBase.cursor()
    Cursor.execute("select column_type from information_schema.columns where table_schema='{ss}' and column_name='{s}' and table_name='{j}';".format(s=column_name,ss=table_schema,j=table_name))
    r = Cursor.fetchall()
    for rr in r:
        rr=rr[0].decode()
    return rr


def exc(s,df,p,table_name,target,source):
    # Cursor= DataBase.cursor()
    # Cursor.execute("select data_type from information_schema.columns where table_schema='target' and table_name='info' and column_name='{}'".format(s))
    if(source!='' and target!='' and s!=''):
        c = check_d(s, target, table_name)
        # print(c)
        d = check_d(s, source, table_name)
        # print(d)
        # for x in c:
        ee = [c]
        e = [s]
        eeee = [d]
        eeeee = [table_name]
        eeeeee = [p]
    else:
        ee=['']
        e=['']
        eeee=['']
        eeeee=[table_name]
        eeeeee=[p]
    df1=pd.DataFrame(e,columns=['target'])
    df2=pd.DataFrame(ee,columns=['target_datatype'])
    df3=pd.DataFrame(e,columns=['source'])
    df4=pd.DataFrame(eeee,columns=['source_datatype'])
    df5=pd.DataFrame(eeeee,columns=['Table_name'])
    df6=pd.DataFrame(eeeeee,columns=['ddl'])
    df7=pd.concat([df5,df3,df4,df1,df2,df6],axis=1)
    df=pd.concat([df,df7],ignore_index=True)
    print(df)
    return df


def Check_Change_DateTime_Datatype(source_name,target_name,table_name,df):
    Cursor=DataBase.cursor()
    Cursor.execute("select column_name from information_schema.columns where table_schema='{pp}';".format(pp=target_name))
    records=Cursor.fetchall()
    c=0
    try:
        for record in records:
            c=0
            for s in record:
                print(s)
                Cursor.execute("select {p} from {pp}.{ppp};".format(p=s,pp=target_name,ppp=table_name))
                rr=Cursor.fetchall()
                for p in rr:
                    print(p)
                    if(p==(None,)):
                        c=c+1
            if(c>0):
                print(s)
                k="alter table {t}.{tt} modify {ttt} datetime;".format(t=target_name,tt=table_name,ttt=s)
                df = exc(s, df, k, table_name,target_name , source_name)
                # df.to_csv("t1.csv",sheet_name='datetime', index=False)
                Cursor.execute(k)
                return df
    except Exception as err:
        print(err)


def Check_columns(table_name,source_name,target_name):
    Cursor=DataBase.cursor()
    Cursor.execute("select column_name from information_schema.columns where table_schema='{k}' and table_name='{kk}';".format(k=source_name,kk=table_name))
    r=Cursor.fetchall()
    Cursor.execute("select column_name from information_schema.columns where table_schema='{k}' and table_name='{kk}';".format(k=target_name,kk=table_name))
    r1=Cursor.fetchall()
    if(r!=r1):
        # p=check_d(r)
        Cursor.execute("drop table {i}.{ii};".format(i=target_name,ii=table_name))
        Cursor.execute("create table {ss}.{s} LIKE {sss}.{s};".format(s=table_name,ss=target_name,sss=source_name))
        # Cursor.execute("alter table {tt}.{t} add {ttt}{tttt};".format(tt=target_name,t=table_name,ttt=r,tttt=p) )


def Create_Database(source_name,target_name,table_name,df):
    Cursor = DataBase.cursor()
    Cursor.execute("select * from {t}.{tt};".format(t=source_name,tt=table_name))
    records=Cursor.fetchall()
    Cursor.execute("select column_name from information_schema.columns where table_name='{f}' and table_schema='{ff}';".format(f=table_name,ff=source_name))
    r=Cursor.fetchall()
    a=''
    b=''
    c=''
    for rr in r:
        a=a.join(rr)
        b=b+a
        b=b+','
        c=c+'%s,'
    b=b.rstrip(b[-1])
    c=c.rstrip(c[-1])
    # print(b)
    # print(c)
    Cursor.execute("select {col} from {t}.{tt};".format(t=source_name, tt=table_name, col=b))
    rec = Cursor.fetchall()
    # print(rec)
    for record in rec:
        # print(record)
        try:
            cor=DataBase.cursor()
            k=''
            smt=("insert into {tttt}.{ttt} ({k}) values ({kk});".format(tttt=target_name,ttt=table_name,k=b,kk=c))
            df = exc(k, df, smt, table_name, k, k)
            cor.execute(smt,record)
            return df
        except Exception as err:
            print(err)


def Check_Datatype(tt,source_name,target_name,df):
    Cursor=DataBase.cursor()
    Cursor.execute("select column_name from information_schema.columns where table_schema='{tar}' and table_name='{t}';".format(t=tt,tar=target_name))
    record1=Cursor.fetchall()
    c=0
    for record in record1:
        try:
            for s in record:
                c=0
                r1=check_d(s,target_name,tt)
                r2=check_d(s,source_name,tt)
                if((r1!=r2)):
                    c=c+1
            if(c>0):
                # Cursor.execute("drop table {i}.{ii};".format(i=target_name, ii=tt))
                # Cursor.execute("create table {ss}.{s} LIKE {sss}.{s};".format(s=tt, ss=target_name, sss=source_name))
                query="alter table {tg}.{r} modify {e} {r2};".format(r=tt,e=s,r2=r2,tg=target_name)
                df = exc(s, df, query, tt, target_name, source_name)
                # df.to_csv("t1.csv",sheet_name='datatype', index=False)
                Cursor.execute(query)
                return df
        except Exception as err:
            print(err)


def Comp_Database(source_name,target_name,df):
    source_schema={}
    a=1
    Cursor=DataBase.cursor()
    Cursor.execute("select table_name from information_schema.tables where table_schema='{t}';".format(t=source_name))
    records=(Cursor.fetchall())
    # print(records)
    try:
        for row in iter(records):
            for r in row:
                # print(r)
                source_schema[a]=r
                a=a+1
        print(source_schema)
        target_schema={}
        b=1
        Cursor=DataBase.cursor()
        Cursor.execute("select table_name from information_schema.tables where table_schema='{tt}';".format(tt=target_name))
        record=(Cursor.fetchall())
        # print(records)
        for row in iter(record):
            for re in row:
                # print(r)
                target_schema[b]=re
                b=b+1
        print(target_schema)
        c=source_schema.values()
        e=target_schema.values()
        print(e)
        for d in c:
            g=0
            for f in e:
                if(f==d):
                    g=g+1
            if(g!=1):
                j="create table {ss}.{s} LIKE {sss}.{s};".format(s=d,ss=target_name,sss=source_name)
                jj=''
                # jjj={}
                df = exc(jj, df, j, d,jj , jj)
                # df.to_csv("t1.csv", index=False)
                Cursor.execute(j)
                # return df
        Cursor.execute("select table_name from information_schema.tables where table_schema='{tt}';".format(tt=target_name))
        record=(Cursor.fetchall())
        # print(records)
        for row in iter(record):
            for re in row:
                # print(r)
                target_schema[b]=re
                b=b+1
        e=target_schema.values()
        for f in e:
            Check_columns(f,source_name,target_name)
        for f in e:
            df=Check_Datatype(f,source_name,target_name,df)
        for f in e:
            df=Create_Database(source_name,target_name,f,df)
        for f in e:
            df=Check_Change_DateTime_Datatype(source_name,target_name,f,df)
        return df
    except Exception as err:
        print(err)


if __name__ == '__main__':
    ee = []
    df = pd.DataFrame(ee)
    df=Comp_Database('assign','target',df)
    df.to_csv("t1.csv", index=False)
    # mail('sanchitjn27@gmail.com', 'test', 'target', 't1.csv')