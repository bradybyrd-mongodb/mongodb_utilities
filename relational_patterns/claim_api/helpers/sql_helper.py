#  SQL Routines to help with queries
import psycopg2
import datetime


def sql_query(sql, conn):
    # Simple query executor
    result = {"num_records" : 0, "data" : []}
    start = datetime.datetime.now()
    cur = conn.cursor()
    print(sql)
    try:
        cur.execute(sql)
        row_count = cur.rowcount
        print(f'{row_count} records')
        result = {"num_records" : row_count, "data" : cur.fetchall()}
    except psycopg2.DatabaseError as err:
        print(f'{sql} - {err}')
    cur.close()
    end = datetime.datetime.now()
    elapsed = end - start
    secs = (elapsed.seconds) + elapsed.microseconds * .000001
    print(f"QueryElapsed: {format(secs,'f')} cnt: {result['num_records']}")

    return result

def column_names(table, conn):
    sql = f'SELECT column_name FROM information_schema.columns WHERE table_schema = \'public\' AND table_name   = \'{table}\''
    cur = conn.cursor()
    #print(sql)
    try:
        cur.execute(sql)
        row_count = cur.rowcount
        print(f'{row_count} columns')
    except psycopg2.DatabaseError as err:
        print(f'{sql} - {err}')
    rows = cur.fetchall()
    result = []
    for i in rows:
        result.append(i[0])
    cur.close()
    return result
