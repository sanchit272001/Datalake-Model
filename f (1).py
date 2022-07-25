import mysql.connector
import pandas as pd

DataBase = mysql.connector.connect(
    host="localhost",
    user="root",
    password="mynewpassword",
    autocommit=True
)


def Execute_Query(schema, table):
    Cursor = DataBase.cursor()
    Cursor.execute("select Column_Name,Column_Type,Column_Key from information_schema.columns where table_schema='{a}' and table_name='{t}' ORDER BY ORDINAL_POSITION ASC;".format(a=schema, t=table))
    df = pd.DataFrame(Cursor.fetchall())
    return df


def Get_Column_Datatype(schema, table):
    a = {}
    b = Execute_Query(schema, table).get(0)
    c = Execute_Query(schema, table).get(1)
    d = len(b)
    for x in range(d):
        a[b[x]] = (c[x]).decode()
    return a


def Create_Column(source_schema, source_table, target_schema, target_table):
    a = (set(Get_Column_Datatype(source_schema, source_table))) - (
        set(Get_Column_Datatype(target_schema, target_table)))
    if a != 0:
        Cursor = DataBase.cursor()
        for x in a:
            Cursor.execute("alter table {ts}.{tt} add ({a} {b});".format(ts=target_schema, tt=target_table, a=x,
                                                                         b=Get_Column_Datatype(source_schema,
                                                                                               source_table).get(x)))
    else:
        print("No Unique Column Found")


def Check_Update_Datatype(source_schema, source_table, target_schema, target_table):
    Cursor = DataBase.cursor()
    for x in Get_Column_Datatype(source_schema, source_table).keys():
        if (Get_Column_Datatype(source_schema, source_table).get(x) != Get_Column_Datatype(target_schema,
                                                                                           target_table).get(x)):
            Cursor.execute("alter table {ts}.{tt} modify {cn} {ct};".format(ts=target_schema, tt=target_table, cn=x,
                                                                            ct=Get_Column_Datatype(source_schema,
                                                                                                   source_table).get(
                                                                                x)))


def Check_PrimaryKey(source_schema, source_table):
    a = {}
    b = Execute_Query(source_schema, source_table).get(0)
    c = Execute_Query(source_schema, source_table).get(2)
    d = len(b)
    for x in range(d):
        a[b[x]] = (c[x])
    key = [k for k, v in a.items() if v == 'PRI']
    keys = ' '.join([str(s) for s in key])
    return keys


def Check_Insert_Data(source_schema, source_table, target_schema, target_table):
    Cursor = DataBase.cursor()
    a = Check_PrimaryKey(source_schema, source_table)
    Cursor.execute("select {pk} from {ss}.{st}".format(pk=a, ss=source_schema, st=source_table))
    df = pd.DataFrame(Cursor.fetchall())
    Cursor.execute("select {pk} from {ts}.{tt}".format(pk=a, ts=target_schema, tt=target_table))
    df1 = pd.DataFrame(Cursor.fetchall())
    b = df.get(0).to_dict()
    c = df1.get(0).to_dict()
    d = set(b) - set(c)
    for x in d:
        Cursor.execute(
            "select * from {ss}.{st} where {pk}={x};".format(pk=a, x=b[x], ss=source_schema, st=source_table))
        df2 = Cursor.fetchall()
        print(df2)
        s = Execute_Query(source_schema, source_table).get(0)
        print(s)
        v = ''
        w = ''
        for m in s:
            v = v + m + ','
            w = w + '%s,'
        v = v.rstrip(v[-1])
        w = w.rstrip(w[-1])
        smt = ("insert into {ts}.{tt} ({k}) values ({kk});".format(ts=target_schema, tt=target_table, k=v, kk=w))
        Cursor.execute(smt, df2[0])


def Check_datetime(target_schema, target_table):
    Cursor = DataBase.cursor()
    for x in Get_Column_Datatype(target_schema, target_table).keys():
        if Get_Column_Datatype(target_schema, target_table).get(x) == "timestamp":
            Cursor.execute(
                "alter table {ts}.{tt} modify {cn} datetime;".format(ts=target_schema, tt=target_table, cn=x))


def Compare_Table(source_schema, target_schema):
    Cursor = DataBase.cursor()
    Cursor.execute("select table_name from information_schema.tables as Table_list where table_schema='{t}';".format(
        t=source_schema))
    df1 = pd.DataFrame(Cursor.fetchall())
    Cursor.execute("select table_name from information_schema.tables as Table_list where table_schema='{t}';".format(
        t=target_schema))
    df2 = pd.DataFrame(Cursor.fetchall())
    df = df1.merge(df2, how='left', indicator=True)
    a = len(df.index)
    d = (df.get('_merge')).to_dict()  # making dictionary of MERGE COLUMN
    e = (df.get(0)).to_dict()  # making dictionary of list of tables
    for x in range(a):
        if d[x] == "both":
            Create_Column(source_schema=source_schema, source_table=e[x], target_schema=target_schema,
                          target_table=e[x])
            Check_Update_Datatype(source_schema=source_schema, source_table=e[x], target_schema=target_schema,
                                  target_table=e[x])
            Check_datetime(target_schema=target_schema, target_table=e[x])
            Check_Insert_Data(source_schema=source_schema, source_table=e[x], target_schema=target_schema,
                              target_table=e[x])
        else:
            Cursor.execute("create table {ts}.{s} LIKE {ss}.{s};".format(s=e[x], ts=target_schema, ss=source_schema))
            Cursor.execute(
                "INSERT INTO {ts}.{s} SELECT * FROM {ss}.{s};".format(ts=target_schema, s=e[x], ss=source_schema))
        x += 1


if __name__ == '__main__':
    Compare_Table('assign', 'target')