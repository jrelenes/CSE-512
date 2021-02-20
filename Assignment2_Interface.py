#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def Sorthelper(i, cur,lower,InputTable,SortingColumnName,OutputTable,upper,openconnection,max_val):

    cur.execute("CREATE TABLE " + str(OutputTable)+str(i)+ " (data real);")
    openconnection.commit()

    if upper == max_val:
        cur.execute("WITH temp AS (SELECT "+str(SortingColumnName)+" FROM "+str(InputTable)+" "
        "WHERE "+ str(SortingColumnName)+" >= " + str(float(lower)) + "AND "+str(SortingColumnName)+" "
        "<= "+str(float(upper))+" ORDER BY "+str(SortingColumnName)+" ASC) INSERT INTO " + str(OutputTable)+str(i) + " SELECT "
        "* FROM temp ")
        openconnection.commit()

    else:
        cur.execute("WITH temp AS (SELECT "+str(SortingColumnName)+" FROM "+str(InputTable)+" "
        "WHERE "+ str(SortingColumnName)+" >= " + str(float(lower)) + "AND "+str(SortingColumnName)+" "
        "< "+str(float(upper))+" ORDER BY "+str(SortingColumnName)+") INSERT INTO " + str(OutputTable)+str(i) +
        " SELECT * FROM temp ")
        openconnection.commit()


def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    cur = openconnection.cursor()
    cur.execute("CREATE TABLE "+str(OutputTable)+" (data real);")
    openconnection.commit()
    cur.execute("SELECT MAX ("+str(SortingColumnName)+") FROM "+str(InputTable)+";")
    upper = cur.fetchall()[0][0]
    max_val = upper
    cur.execute("SELECT MIN ("+str(SortingColumnName)+") FROM "+str(InputTable)+";")
    lower = cur.fetchall()[0][0]
    delta = float((upper - lower) / 5)

    upper = lower + delta
    t =[]
    for i in range(1,6):
        u = threading.Thread(target=Sorthelper, args=(i, cur,lower,InputTable,SortingColumnName,OutputTable,upper,openconnection,max_val))
        u.start()
        t.append(u)
        cur.execute("SELECT MAX (" + str(SortingColumnName) + ") FROM " + str(InputTable) + ";")
        lower = upper
        upper += delta

    for i in t:
        i.join()

    for i in range(1, 6):
        cur.execute("WITH temp AS (SELECT * FROM " + str(OutputTable)+str(i) + " ) INSERT INTO " + str(OutputTable)+" SELECT * FROM temp ")
        openconnection.commit()
        cur.execute("DROP TABLE " + str(OutputTable)+str(i))
        openconnection.commit()








def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    #same process
    #range parition for table one and table 2, 5 processors per table
    pass # Remove this once you are done with implementation


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


