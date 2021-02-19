#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()        
def Sorthelper(cur,i,InputTable,SortingColumnName,OutputTable):
	#it will be calling for both a min and max number like range parition (use same logic)
	#The min and max are updated in global table here and the main table log record will be kept centrally
	cur = openconnection.cursor()
	cur.execute("INSERT INTO metadata_table VALUES ('rangePartition', "+str(i)+")")
	cur.execute('CREATE TABLE range_ratings_part'+str(i)+'(userid integer, movieid integer, rating float)')
	openconnection.commit()
	if i == 0:
		cur.execute('INSERT INTO '+str(OutputTable)+''+str(i) +' SELECT * FROM ' + InputTable+' WHERE rating >= '+str(0)+'AND rating <= '+str(1)+' ORDER BY rating ASC;')
	else:
		cur.execute('INSERT INTO '+str(OutputTable)+''+str(i) +' SELECT * FROM ' + InputTable+' WHERE rating > '+str(lower)+'AND rating <= '+str(upper)+' ORDER BY rating ASC;')
		    
		    
		    

def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    #here the log record gets documented
    #the min max is evaluated for the whole table and updated partition 
    #wise and it will have float values ranges exaclty like range partition
    cur.execute("CREATE TABLE "+str(OutputTable)+"();")
    cur.execute('CREATE TABLE metadata_table (table_name VARCHAR, number_of_partitions integer, index integer DEFAULT 0);')
    openconnection.commit()
    cur = openconnection.cursor()
    # thread 1-5 in python for slices 1-5
    for i in range(5):
        t = threading.Thread(target=Sorthelper, args=(cur,i,InputTable,SortingColumnName,OutputTable))
        t.start()







    
    #cur.execute("SELECT * FROM "+str(OutputTable)+";")
    #print(cur.fetchall())
    #pass #Remove this once you are done with implementation

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
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


