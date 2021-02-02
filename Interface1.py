import psycopg2
import os
import sys


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("CREATE TABLE "+ratingstablename+ " (userid int, movieid int, rating float);")
    with open(ratingsfilepath, 'r') as f:
        for line in f.readlines():
            columns = line.split('::')
            cur.execute('INSERT INTO ' + ratingstablename + ' VALUES (%s, %s, %s)', (columns[0], columns[1], columns[2]))
        f.close()

    openconnection.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
     cur = openconnection.cursor()
     cur.execute('SELECT COUNT(*) FROM ' + ratingstablename)
     p = cur.fetchall()[0][0]
     if p % numberofpartitions == 0:
         const = int(p / numberofpartitions)
         skip = 0
         last = p - const
         for i in range(numberofpartitions):
          cur.execute('CREATE TABLE rangePartition'+str(i)+'(userid int, movieid int, rating float)')
          openconnection.commit()
          cur.execute('INSERT INTO rangePartition'+str(i) +' SELECT * FROM ' + ratingstablename+' OFFSET '+str(skip)+' LIMIT '+str(const)+';')
          openconnection.commit()
          skip += const
     else:
         print('The table count is odd')

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
     cur = openconnection.cursor()

     cur.execute('SELECT COUNT(*) FROM '+str(ratingstablename))
     i = cur.fetchall()[0][0]
     skip = 0
     for k in range(numberofpartitions):
            cur.execute('CREATE TABLE round_robin'+str(k)+'(userid int, movieid int, rating float)')
            openconnection.commit()
     while i > 0:
        for j in range(numberofpartitions):
            cur.execute('INSERT INTO round_robin'+str(j)+' SELECT * FROM ' + ratingstablename+ ' LIMIT 1 OFFSET '+str(skip))
            skip += 1
            openconnection.commit()
        i -= numberofpartitions

    # cur.execute('WITH round_robin_total(
    # for k in numberofpartitions:
    #	SELECT * FROM ' + round_robin+str(k) +';')
    #pass  # Remove this once you are done with implementation


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    # cur = openconnection.cursor
    # cur.execute('INSERT INTO '+ ratingstablename+'(*) 'SELECT * FROM '+ round_robin_total)
    pass  # Remove this once you are done with implementation


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    pass  # Remove this once you are done with implementation


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    pass  # Remove this once you are done with implementation


def pointQuery(ratingValue, openconnection, outputPath):
    pass  # Remove this once you are done with implementation


def createDB(dbname='dds_assignment1'):
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
    con.close()


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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()


ratingstablename = 'ratings'
ratingsfilepath = '/home/not-yours/Documents/ml-10M100K/' + ratingstablename + '.dat'
# conn = getOpenConnection()
# cur = conn.cursor()
#loadRatings(ratingstablename, ratingsfilepath, getOpenConnection())
rangePartition(ratingstablename,6,getOpenConnection())
# print(cur.execute("SELECT * FROM test;"))
# print(cur.fetchall())
#roundRobinPartition(ratingstablename,6,getOpenConnection())