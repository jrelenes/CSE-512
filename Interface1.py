import psycopg2
import os
import sys
RANGE_TABLE_PREFIX = 3
RROBIN_TABLE_PREFIX = 3

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
     global RANGE_TABLE_PREFIX
     if RANGE_TABLE_PREFIX < numberofpartitions:
        RANGE_TABLE_PREFIX = numberofpartitions
     p = cur.fetchall()[0][0]
     if p % numberofpartitions == 0:
         const = int(p / numberofpartitions)
         skip = 0
         last = p - const
         for i in range(numberofpartitions):
          cur.execute('CREATE TABLE range_ratings_part'+str(i)+'(userid int, movieid int, rating float)')
          openconnection.commit()
          cur.execute('INSERT INTO range_ratings_part'+str(i) +' SELECT * FROM ' + ratingstablename+' ORDER BY rating ASC OFFSET '+str(skip)+' LIMIT '+str(const)+';')
          openconnection.commit()
          skip += const
     else:
         print('The table count is odd')

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
     cur = openconnection.cursor()
     global RROBIN_TABLE_PREFIX
     if RROBIN_TABLE_PREFIX < numberofpartitions:
        RROBIN_TABLE_PREFIX = numberofpartitions
     cur.execute('SELECT COUNT(*) FROM '+str(ratingstablename))
     i = cur.fetchall()[0][0]
     skip = 0
     for k in range(numberofpartitions):
            cur.execute('CREATE TABLE round_robin_ratings_part'+str(k)+'(userid int, movieid int, rating float)')
            openconnection.commit()
     while i > 0:
        for j in range(numberofpartitions):
            cur.execute('INSERT INTO round_robin_ratings_part'+str(j)+' SELECT * FROM ' + ratingstablename+ ' ORDER BY rating ASC LIMIT 1 OFFSET '+str(skip))

            skip += 1
            openconnection.commit()
        i -= numberofpartitions

def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
     cur = openconnection.cursor
     cur.execute(''' select count(*) from information_schema.tables where tables.table_name LIKE '%round_robin_ratings_part%'; ''')
     #print(cur.fetchall())
     #cur.execute('INSERT INTO '+ ratingstablename+ ' VALUES (%d,%d,%f), ('+userid+','+ itemid+','+rating+');')


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    #cur = openconnection.cursor()
    #cur.execute("SELECT * FROM pg_catalog.pg_tables")
    pass  # Remove this once you are done with implementation


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    global RROBIN_TABLE_PREFIX
    global RANGE_TABLE_PREFIX
    cur = openconnection.cursor()

    with open("/home/not-yours/Documents/Assignment1/table.txt", "w") as file:
        for i in range(RROBIN_TABLE_PREFIX):
            name = "'round_robin_ratings_part"+str(i)+"'"
            cur.execute('ALTER TABLE round_robin_ratings_part'+str(i)+' ADD COLUMN table_name1 VARCHAR default '+ name)
            openconnection.commit()
            sql = "COPY (WITH temp AS (SELECT table_name1, userid, movieid, " \
                  "rating FROM round_robin_ratings_part"+str(i)+") SELECT * from temp WHERE rating >="+str(ratingMinValue)+" AND rating <= "+str(ratingMaxValue)+") TO STDOUT WITH CSV DELIMITER ','"
            cur.copy_expert(sql, file)
        for i in range(RANGE_TABLE_PREFIX):
            name = "'range_ratings_part"+str(i)+"'"
            cur.execute('ALTER TABLE range_ratings_part'+str(i)+' ADD COLUMN table_name2 VARCHAR default '+ name)
            openconnection.commit()
            sql = "COPY (WITH temp AS (SELECT table_name2, userid, movieid, rating FROM range_ratings_part"+str(i)+" WHERE rating >= "+str(ratingMinValue)+" AND rating <= "+str(ratingMaxValue)+" ) SELECT * from temp) TO STDOUT WITH CSV DELIMITER ','"
            cur.copy_expert(sql, file)
            
def pointQuery(ratingValue, openconnection, outputPath):
    global RROBIN_TABLE_PREFIX
    global RANGE_TABLE_PREFIX
    cur = openconnection.cursor()
    #cur.execute('CREATE TABLE output_point_query (PartitionName varchar, UserID int, MovieID int, Rating int);')
    #openconnection.commit()

    for i in range(RROBIN_TABLE_PREFIX):
        cur.execute(
            'INSERT INTO output_point_query (UserID,MovieID, Rating ) SELECT * FROM round_robin_ratings_part' + str(i) + ';')
        cur.execute('UPDATE output_point_query SET PartitionName=%s WHERE PartitionName IS NULL',
                    ('round_robin_ratings_part' + str(i),))
        openconnection.commit()
    for i in range(RANGE_TABLE_PREFIX):
        cur.execute('INSERT INTO output_point_query (UserID,MovieID, Rating ) SELECT * FROM range_ratings_part' + str(i) + ';')
        cur.execute('UPDATE output_point_query SET PartitionName=%s WHERE PartitionName IS NULL', ('range_ratings_part' + str(i),))
        openconnection.commit()

    cur.execute('SELECT * FROM output_point_query WHERE Rating = ' + str(ratingValue))
    sql = "COPY (SELECT * FROM output_point_query) TO STDOUT WITH CSV DELIMITER ','"
    with open("/home/not-yours/Documents/Assignment1/table.txt", "w") as file:
        cur.copy_expert(sql, file)

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
roundRobinPartition(ratingstablename,6,getOpenConnection())
#roundRobinInsert(ratingstablename,1,364,1,getOpenConnection())
rangeQuery(3,4,getOpenConnection(),'/home/not-yours/Documents/ml-10M100K/')
#pointQuery(5,getOpenConnection(),'/home/not-yours/Documents/ml-10M100K/')