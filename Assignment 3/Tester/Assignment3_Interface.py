#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    cur = openconnection.cursor()
    def thread_sort(input_table, sorting_column, connection, partition_name, lb, ub):
        cur1 = connection.cursor()
        cur1.execute('CREATE TABLE {0}  AS (SELECT * FROM {1} WHERE 5=6)'.format(partition_name, input_table))
        if lb==mini:
            cur1.execute('INSERT INTO {0} SELECT * FROM {1} WHERE {2}>={3} and {2}<={4} ORDER BY {2} ASC'
                         .format(partition_name, input_table, sorting_column, lb, ub))
        else:
            cur1.execute('INSERT INTO {0} SELECT * FROM {1} WHERE {2}>{3} and {2}<={4} ORDER BY {2} ASC'
                         .format(partition_name, input_table, sorting_column, lb, ub))
        connection.commit()

    no_threads = 5
    # finding minimum value
    cur.execute('SELECT MIN({0}) FROM {1}'.format(SortingColumnName, InputTable))
    mini = cur.fetchone()[0]
    # finding maximum value
    cur.execute('SELECT MAX({0}) FROM {1}'.format(SortingColumnName, InputTable))
    maxi = cur.fetchone()[0]
   # print(mini, maxi)
    interval = (maxi-mini)/float(no_threads)
    lb = mini
    ub = lb + interval
    thread_Arr = [0] * no_threads
    for i in range(no_threads):
        partition_name = 'partition' +  str(i)
        thread_Arr[i] = threading.Thread(target=thread_sort(InputTable, SortingColumnName, openconnection, partition_name, lb, ub))
        thread_Arr[i].start()
        lb = ub
        ub = lb + interval

    for j in range(no_threads):
        thread_Arr[j].join()

    #creating output table
    cur.execute('CREATE TABLE {0} AS (SELECT * FROM {1} WHERE 5=6)'.format(OutputTable, InputTable))

    for t in range(no_threads):
        partition_name = 'partition' + str(t)
        cur.execute('INSERT INTO {0} SELECT * FROM {1}'.format(OutputTable, partition_name))
        cur.execute('DROP TABLE {0}'.format(partition_name))
    openconnection.commit()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    cur = openconnection.cursor()
    no_threads = 5

    def join_partition(input_table1, input_table2, col_1, col_2, lb, ub, connection, partition):
        cur1 = connection.cursor()
        if lb == mini:
            cur1.execute('CREATE TABLE {0} AS (SELECT * FROM {1} as t1, {2} as t2 WHERE t1.{3}>={5} and t2.{4}>={5} '
                         'and t1.{3}<={6} and t2.{4}<={6} and t1.{3}=t2.{4} )'
                         .format(partition_name, input_table1, input_table2, col_1, col_2, lb, ub))
        else:
            cur1.execute('CREATE TABLE {0} AS (SELECT * FROM {1} as t1, {2} as t2 WHERE t1.{3}>{5} and t2.{4}>{5} '
                         'and t1.{3}<={6} and t2.{4}<={6} and t1.{3}=t2.{4} )'
                         .format(partition_name, input_table1, input_table2, col_1, col_2, lb, ub))
        connection.commit()

    #finding minimum value of commmon attribute from input table1
    cur.execute('SELECT MIN({0}) FROM {1}'.format(Table1JoinColumn, InputTable1))
    mini_t1 = cur.fetchone()[0]
    #finding minimum value of commmon attribute from input table2
    cur.execute('SELECT MIN({0}) FROM {1}'.format(Table2JoinColumn, InputTable2))
    mini_t2 = cur.fetchone()[0]
    # finding minimum from both tables
    if mini_t1<mini_t2:
        mini = mini_t1
    else:
        mini = mini_t2

    # finding maximum value of commmon attribute from input table1
    cur.execute('SELECT MAX({0}) FROM {1}'.format(Table1JoinColumn, InputTable1))
    maxi_t1 = cur.fetchone()[0]
    # finding maximum value of commmon attribute from input table2
    cur.execute('SELECT MAX({0}) FROM {1}'.format(Table2JoinColumn, InputTable2))
    maxi_t2 = cur.fetchone()[0]
    # finding minimum from both tables
    if maxi_t1 > maxi_t2:
        maxi = maxi_t1
    else:
        maxi = maxi_t2

    interval = (maxi-mini)/float(no_threads)
    lb = mini
    ub = mini + interval
    thread_Arr = [0]*no_threads
    for i in range(no_threads):
        partition_name = 'partition' + str(i)
        thread_Arr[i] = threading.Thread(target=join_partition(InputTable1, InputTable2, Table1JoinColumn,
                                                                Table2JoinColumn, lb, ub, openconnection, partition_name))
        thread_Arr[i].start()
        lb = ub
        ub = lb + interval

    for j in range(no_threads):
        thread_Arr[j].join()

    #creating output table
    cur.execute('CREATE TABLE {0} AS (SELECT * FROM {1},{2} WHERE 5=6)'.format(OutputTable, InputTable1, InputTable2))

    for t in range(no_threads):
        partition_name = 'partition' + str(t)
        cur.execute('INSERT INTO {0} SELECT * FROM {1}'.format(OutputTable, partition_name))
        cur.execute('DROP TABLE {0}'.format(partition_name))
    openconnection.commit()


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
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
        print 'A database named {0} already exists'.format(dbname)

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
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
