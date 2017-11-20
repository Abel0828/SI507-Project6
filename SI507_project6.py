# Import statements
import psycopg2
import psycopg2.extras
import sys
from config import *
import csv

DEBUG=True
def print_result(result):
    if type(result)==int:
        print(result)
    else:
        for element in result:
            print(element)

# Write code / functions to set up database connection and cursor here.
def get_connection_and_cursor():
    print(db_password)
    try:
        
        if db_password != "":
            print("connecting to "+"dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
            db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
            print("Successfully connecting to database.")
        else:
            print("connecting to "+"dbname='{0}' user='{1}'".format(db_name, db_user))
            db_connection = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
    except:
        print("Unable to connect to the database. Check server and credentials.")
        sys.exit(1) # Stop running program if there's no db connection.

    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor

conn, cur = get_connection_and_cursor()

# Write code / functions to create tables with the columns you want and all database setup here.
cur.execute("""
    CREATE TABLE IF NOT EXISTS "States"(
    "ID" SERIAL,
    "Name" VARCHAR(40) UNIQUE,
    PRIMARY KEY ("ID")
    );"""
)
conn.commit()
cur.execute("""
    CREATE TABLE IF NOT EXISTS "Sites"(
    "ID" SERIAL,
    "Name" VARCHAR(128) UNIQUE,
    "Type" VARCHAR(128),
    "State_ID" INTEGER,
    "Location" VARCHAR(255),
    "Description" TEXT,
    FOREIGN KEY ("State_ID") REFERENCES "States" ("ID")
    );"""
)

# Write code / functions to deal with CSV files and insert data into the database here.
cur.executemany("""INSERT INTO "States" ("Name") VALUES(%s)""",[("Arkansas",),("Michigan",),("California",)])
f1=open('arkansas.csv','r',encoding='utf-8')
f2=open('michigan.csv','r',encoding='utf-8')
f3=open('california.csv','r',encoding='utf-8')
reader1=csv.DictReader(f1,delimiter=',',quotechar = '"')
reader2=csv.DictReader(f2,delimiter=',',quotechar = '"')
reader3=csv.DictReader(f3,delimiter=',',quotechar = '"')

for i,reader in enumerate((reader1,reader2,reader3)):
    for row in reader:
        line_tup=(row['NAME'],row['TYPE'],i+1,row['ADDRESS'],row['DESCRIPTION'])
        #print(row['NAME'])
        cur.execute("""INSERT INTO "Sites" ("Name","Type","State_ID","Location","Description") VALUES(%s,%s,%s,%s,%s)""",line_tup)
        
# Make sure to commit your database changes with .commit() on the database connection.
conn.commit()


# Write code to be invoked here (e.g. invoking any functions you wrote above)



# Write code to make queries and save data in variables here.
cur.execute("""SELECT "Location" from "Sites"; """)
all_locations=cur.fetchall()
if(DEBUG):
    print_result(all_locations)
    
cur.execute("""SELECT "Name" from "Sites" WHERE "Description" LIKE '%beautiful%'; """)
beautiful_sites=cur.fetchall()
if(DEBUG):
    print_result(beautiful_sites)
    
cur.execute("""SELECT COUNT(*) from "Sites" WHERE "Type" = 'National Lakeshore';""")
natl_lakeshores=cur.fetchall()
if(DEBUG):
    print_result(natl_lakeshores)

cur.execute("""SELECT
    "Sites"."Name" AS "Name"
    from "Sites"
    INNER JOIN "States"
    ON "Sites"."State_ID"="States"."ID"
    WHERE "States"."Name"='Michigan';
    """)
michigan_names=cur.fetchall()

cur.execute("""SELECT
    COUNT(*)
    from "Sites"
    WHERE "ID"=(SELECT "State_ID" FROM "States" WHERE "Name"='Arkansas');
    """)
total_number_arkansas=cur.fetchall()




f1.close()
f2.close()
f3.close()
# We have not provided any tests, but you could write your own in this file or another file, if you want.
