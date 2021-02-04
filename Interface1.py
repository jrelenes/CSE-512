import psycopg2
import os
import sys

#RangeRating -- create table to track metadata
#input and outputh path is provided
#range partition 1partition (0- less that 1 rating)
#2 paritions (0- less that 1 rating) and (1 - less than 2 ratings ...etc)

#two scenarios (or not an and) not both conditions(2)
# load -> partition ->insert -> delete all(rr)
# load -> partition ->insert -> delete all(range)
# for queries the main table types are not empty but a specific partition type can be empty
# both query congtain rr and range simultaneously
# WHEN LOADING THE TABLE ORDER DOESNT MATTER
# 5 MAX RATING / PARTITIONS THEN ADD FROM 0 TO 5 BY THE THRESHOL EXP 5 rating max / 10 partitions rating = 0.5 step per partion max
# 0 - <0.5, 0.5> - 1 ...etc
 
def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("CREATE TABLE "+ratingstablename+ " (userid int, movieid int, rating float);")
    cur.execute('CREATE TABLE metadata_table (table_name VARCHAR, number_of_partitions int);')

    with open(ratingsfilepath, 'r') as f:
        for line in f.readlines():
            columns = line.split('::')
            cur.execute('INSERT INTO ' + ratingstablename + ' VALUES (%s, %s, %s)', (columns[0], columns[1], columns[2]))
        f.close()

    openconnection.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
     cur = openconnection.cursor()
     cur.execute("INSERT INTO metadata_table VALUES ('rangePartition', "+str(numberofpartitions)+")")
     cur.execute('SELECT COUNT(*) FROM ' + ratingstablename)
     init = float(5 / numberofpartitions)
     p = cur.fetchall()[0][0]
     lower = 0
     upper = init
     for i in range(numberofpartitions):
          cur.execute('CREATE TABLE range_ratings_part'+str(i)+'(userid int, movieid int, rating float)')
          openconnection.commit()
          if i == 0:
            cur.execute('INSERT INTO range_ratings_part'+str(i) +' SELECT * FROM ' + ratingstablename+' WHERE rating >= '+str(lower)+'AND rating <= '+str(upper)+' ORDER BY rating ASC;')
          else:
            cur.execute('INSERT INTO range_ratings_part'+str(i) +' SELECT * FROM ' + ratingstablename+' WHERE rating > '+str(lower)+'AND rating <= '+str(upper)+' ORDER BY rating ASC;')

          openconnection.commit()
          lower = upper
          upper += init




def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
     cur = openconnection.cursor()
     cur.execute("INSERT INTO metadata_table VALUES ('roundRobinPartition', " + str(numberofpartitions) + ")")
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
    cur = openconnection.cursor()
    cur.execute("SELECT number_of_partitions FROM metadata_table WHERE table_name = 'rangePartition' ")
    range_number = int(cur.fetchall()[0][0])

    cur.execute("SELECT number_of_partitions FROM metadata_table WHERE table_name = 'roundRobinPartition' ")
    rr_number = int(cur.fetchall()[0][0])

    with open(outputPath, "w") as file:
        for i in range(rr_number):
            name = "'round_robin_ratings_part"+str(i)+"'"
            cur.execute('ALTER TABLE round_robin_ratings_part'+str(i)+' ADD COLUMN table_name1 VARCHAR default '+ name)
            openconnection.commit()
            sql = "COPY (WITH temp AS (SELECT table_name1, userid, movieid, " \
                  "rating FROM round_robin_ratings_part"+str(i)+") SELECT * from temp WHERE rating >="+str(ratingMinValue)+" AND rating <= "+str(ratingMaxValue)+") TO STDOUT WITH CSV DELIMITER ','"
            cur.copy_expert(sql, file)
        for i in range(range_number):
            name = "'range_ratings_part"+str(i)+"'"
            cur.execute('ALTER TABLE range_ratings_part'+str(i)+' ADD COLUMN table_name2 VARCHAR default '+ name)
            openconnection.commit()
            sql = "COPY (WITH temp AS (SELECT table_name2, userid, movieid, rating FROM range_ratings_part"+str(i)+" WHERE rating >= "+str(ratingMinValue)+" AND rating <= "+str(ratingMaxValue)+" ) SELECT * from temp) TO STDOUT WITH CSV DELIMITER ','"
            cur.copy_expert(sql, file)

def pointQuery(ratingValue, openconnection, outputPath):
    cur = openconnection.cursor()
    cur.execute("SELECT number_of_partitions FROM metadata_table WHERE table_name = 'rangePartition' ")
    range_number = int(cur.fetchall()[0][0])

    cur.execute("SELECT number_of_partitions FROM metadata_table WHERE table_name = 'roundRobinPartition' ")
    rr_number = int(cur.fetchall()[0][0])

    with open(outputPath, "w") as file:
        for i in range(rr_number):
            name = "'round_robin_ratings_part" + str(i) + "'"
            cur.execute(
                'ALTER TABLE round_robin_ratings_part' + str(i) + ' ADD COLUMN table_name3 VARCHAR default ' + name)
            openconnection.commit()
            sql = "COPY (WITH temp AS (SELECT table_name3, userid, movieid, " \
                  "rating FROM round_robin_ratings_part" + str(i) + ") SELECT * from temp WHERE rating =" + str(
                ratingValue)+") TO STDOUT WITH CSV DELIMITER ','"
            cur.copy_expert(sql, file)
        for i in range(range_number):
            name = "'range_ratings_part" + str(i) + "'"
            cur.execute('ALTER TABLE range_ratings_part' + str(i) + ' ADD COLUMN table_name4 VARCHAR default ' + name)
            openconnection.commit()
            sql = "COPY (WITH temp AS (SELECT table_name4, userid, movieid, rating FROM range_ratings_part" + str(
                i) + " WHERE rating = " + str(ratingValue)+") SELECT * from temp) TO STDOUT WITH CSV DELIMITER ','"
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


#ratingstablename = 'ratings'
#ratingsfilepath = '/home/not-yours/Documents/ml-10M100K/' + ratingstablename + '.dat'
# conn = getOpenConnection()
# cur = conn.cursor()
#loadRatings(ratingstablename, ratingsfilepath, getOpenConnection())
#rangePartition(ratingstablename,6,getOpenConnection())
# print(cur.execute("SELECT * FROM test;"))
# print(cur.fetchall())
#roundRobinPartition(ratingstablename,6,getOpenConnection())
#roundRobinInsert(ratingstablename,1,364,1,getOpenConnection())
#rangeQuery(3,4,getOpenConnection(),'/home/not-yours/Documents/ml-10M100K/test_range.txt')
#pointQuery(5,getOpenConnection(),'/home/not-yours/Documents/ml-10M100K/')
