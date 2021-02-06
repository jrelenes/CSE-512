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
    cur.execute("SELECT * FROM information_schema.tables WHERE table_name=%s", (ratingstablename,))

    cur.execute(
        "CREATE TABLE " + ratingstablename + " (userid integer, trash_1 varchar, movieid integer, trash_2 varchar, rating numeric(2,1), trash_3 varchar, trash_4 varchar);")
    cur.execute('CREATE TABLE metadata_table (table_name VARCHAR, number_of_partitions int, index int DEFAULT 0);')

    with open(ratingsfilepath) as f:
        cur.copy_from(f, ratingstablename, sep=":")

    cur.execute(
        "ALTER TABLE " + ratingstablename + " DROP COLUMN trash_1, DROP COLUMN trash_2, DROP COLUMN trash_3, DROP COLUMN trash_4;")


    openconnection.commit()
    cur.close()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
     cur = openconnection.cursor()
     cur.execute("INSERT INTO metadata_table VALUES ('rangePartition', "+str(numberofpartitions)+")")
     init = float("{:.2f}".format(5 / numberofpartitions))
     lower = 0
     upper = init
     for i in range(numberofpartitions):
          cur.execute('CREATE TABLE range_ratings_part'+str(i)+'(userid int, movieid int, rating decimal(2,1))')
          openconnection.commit()
          if i == 0:
            cur.execute('INSERT INTO range_ratings_part'+str(i) +' SELECT * FROM ' + ratingstablename+' WHERE rating >= '+str(lower)+'AND rating <= '+str(upper)+' ORDER BY rating ASC;')
          else:
            cur.execute('INSERT INTO range_ratings_part'+str(i) +' SELECT * FROM ' + ratingstablename+' WHERE rating > '+str(lower)+'AND rating <= '+str(upper)+' ORDER BY rating ASC;')

          openconnection.commit()

          if i < numberofpartitions - 1:
            cur.execute("UPDATE metadata_table SET index = " +str(i + 1)+" WHERE table_name = 'rangePartition'")
          else:
            cur.execute("UPDATE metadata_table SET index = " + str(0)+" WHERE table_name = 'rangePartition'")

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
            cur.execute('CREATE TABLE round_robin_ratings_part'+str(k)+'(userid int, movieid int, rating decimal(2,1))')
            openconnection.commit()
     while i > 0:
        for j in range(numberofpartitions):
            cur.execute('INSERT INTO round_robin_ratings_part'+str(j)+' SELECT * FROM ' + ratingstablename+ ' LIMIT 1 OFFSET '+str(skip))

            if j < numberofpartitions - 1:
                cur.execute("UPDATE metadata_table SET index = " + str(j + 1) + " WHERE table_name = 'roundRobinPartition'")
            else:
                cur.execute("UPDATE metadata_table SET index = " + str(0) + " WHERE table_name = 'roundRobinPartition'")

            skip += 1
            openconnection.commit()
        i -= numberofpartitions

def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("INSERT INTO "+str(ratingstablename)+" VALUES ("+str(userid)+","+str(itemid)+","+str(rating)+");")
    openconnection.commit()
    cur.execute("SELECT number_of_partitions FROM metadata_table WHERE table_name = 'roundRobinPartition'")
    numberofpartitions = cur.fetchall()[0][0]
    cur.execute("SELECT index FROM metadata_table WHERE table_name = 'roundRobinPartition' ")
    index = cur.fetchall()[0][0]
    cur.execute("INSERT INTO round_robin_ratings_part" + str(
                index) + " VALUES ("+str(userid)+","+str(itemid)+","+str(rating)+");")
    openconnection.commit()

    if index < numberofpartitions:
        cur.execute("UPDATE metadata_table SET index = " + str(index + 1) + " WHERE table_name = 'roundRobinPartition'")
    else:
        cur.execute("UPDATE metadata_table SET index = " + str(0) + " WHERE table_name = 'roundRobinPartition'")

    openconnection.commit()

def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("INSERT INTO " + str(ratingstablename) + " VALUES (" + str(userid) + "," + str(itemid) + "," + str(
        rating) + ");")
    openconnection.commit()

    cur.execute("SELECT number_of_partitions FROM metadata_table WHERE table_name = 'rangePartition'")
    numberofpartitions = cur.fetchall()[0][0]
    init = float("{:.2f}".format(5 / numberofpartitions))
    lower = 0
    upper = init
    cur.execute("SELECT index FROM metadata_table WHERE table_name = 'rangePartition' ")
    index = int(cur.fetchall()[0][0])
    for i in range(numberofpartitions):
        if i == 0 and rating >= lower and rating <= init:
            cur.execute('INSERT INTO range_ratings_part' + str(
                i) + ' (userid, movieid, rating) VALUES (' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ')')
            openconnection.commit()
            cur.execute("SELECT * FROM range_ratings_part" + str(index))
        elif rating > lower and rating <= upper:
            cur.execute('INSERT INTO range_ratings_part' + str(
                i) + ' (userid, movieid, rating) VALUES (' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ')')
            openconnection.commit()
            cur.execute("SELECT * FROM range_ratings_part" + str(index))

        if i < numberofpartitions - 1:
            cur.execute("UPDATE metadata_table SET index = " + str(i + 1) + " WHERE table_name = 'rangePartition'")
        else:
            cur.execute("UPDATE metadata_table SET index = " + str(0) + " WHERE table_name = 'rangePartition'")

        openconnection.commit()
        lower = upper
        upper += init

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

            #removes 0.0
            cur.execute("CREATE TABLE range_clean (userid integer, movieid integer, rating VARCHAR)")
            cur.execute("INSERT INTO range_clean (userid, movieid, rating) SELECT userid, movieid, rating FROM round_robin_ratings_part"+ str(i)+" WHERE rating >="+str(ratingMinValue)+" AND rating <= "+str(ratingMaxValue)+";")
            cur.execute("UPDATE range_clean SET rating = '0' WHERE rating = '0.0'")
            sql = "COPY (SELECT * from range_clean) TO STDOUT WITH CSV DELIMITER ','"
            cur.copy_expert(sql, file)
            cur.execute("DROP TABLE range_clean;")

        for i in range(range_number):
            name = "'range_ratings_part"+str(i)+"'"
            cur.execute('ALTER TABLE range_ratings_part'+str(i)+' ADD COLUMN table_name2 VARCHAR default '+ name)
            openconnection.commit()

            # removes 0.0
            cur.execute("CREATE TABLE range_clean (userid integer, movieid integer, rating VARCHAR)")
            cur.execute(
                "INSERT INTO range_clean (userid, movieid, rating) SELECT userid, movieid, rating FROM range_ratings_part" + str(
                    i) + " WHERE rating >=" + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue) + ";")
            cur.execute("UPDATE range_clean SET rating = '0' WHERE rating = '0.0'")
            sql = "COPY (SELECT * from range_clean) TO STDOUT WITH CSV DELIMITER ','"
            cur.copy_expert(sql, file)
            cur.execute("DROP TABLE range_clean;")


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

            # removes 0.0
            cur.execute("CREATE TABLE range_clean (userid integer, movieid integer, rating VARCHAR)")
            cur.execute(
                "INSERT INTO range_clean (userid, movieid, rating) SELECT userid, movieid, rating FROM round_robin_ratings_part" + str(
                    i) + " WHERE rating =" + str(ratingValue))
            cur.execute("UPDATE range_clean SET rating = '0' WHERE rating = '0.0'")
            sql = "COPY (SELECT * from range_clean) TO STDOUT WITH CSV DELIMITER ','"
            cur.copy_expert(sql, file)
            cur.execute("DROP TABLE range_clean;")


        for i in range(range_number):
            name = "'range_ratings_part" + str(i) + "'"
            cur.execute('ALTER TABLE range_ratings_part' + str(i) + ' ADD COLUMN table_name4 VARCHAR default ' + name)
            openconnection.commit()

            # removes 0.0
            cur.execute("CREATE TABLE range_clean (userid integer, movieid integer, rating VARCHAR)")
            cur.execute(
                "INSERT INTO range_clean (userid, movieid, rating) SELECT userid, movieid, rating FROM range_ratings_part" + str(
                    i) + " WHERE rating =" + str(ratingValue))
            cur.execute("UPDATE range_clean SET rating = '0' WHERE rating = '0.0'")
            sql = "COPY (SELECT * from range_clean) TO STDOUT WITH CSV DELIMITER ','"
            cur.copy_expert(sql, file)
            cur.execute("DROP TABLE range_clean;")

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



#def deletePartitions(openconnection):

#    cur = openconnection.cursor()
#    cur.execute("SELECT number_of_partitions FROM metadata_table WHERE table_name = 'rangePartition' ")
#     rangePartition = int(cur.fetchall()[0][0])
#     print(rangePartition)
#     cur.execute("SELECT number_of_partitions FROM metadata_table WHERE table_name = 'roundRobinPartition'")
#     roundRobinPartition = int(cur.fetchall()[0][0])
#     print(roundRobinPartition)
#     cur.execute("DROP TABLE metadata_table;")
#
#     for i in range(roundRobinPartition - 1):
       # cursor.execute("DROP TABLE round_robin_ratings_part"+str(i)+";")
    #
    # for j in range(rangePartition - 1):
       # cursor.execute("DROP TABLE range_ratings_part"+str(j)+";")
    #
    # cur.close()