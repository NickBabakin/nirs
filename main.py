import time
import random
import psycopg2

from sqlalchemy import create_engine, text

db_name = 'db'
db_user = 'user'
db_pass = 'passw'
db_host = 'localhost'
db_port = '5432'

# Connect to the database
db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)


def add_new_row(n):
    # Insert a new number into the 'numbers' table.
    with db.engine.begin() as conn:
        conn.execute(text("INSERT INTO numbers (number,timestamp) " + \
                          "VALUES (" + \
                          str(100) + "," + \
                          str(int(round(time.time() * 1000))) + ");"))


def get_response_time(result):
    second_to_last = ""
    last = ""
    for row in result:  # iterating to get last row of answer
        second_to_last = last
        last = row[0]
    return [second_to_last, last]

def get_explain_get_last_row():
    query = "EXPLAIN ANALYZE " + \
            "SELECT number " + \
            "FROM numbers " + \
            "WHERE timestamp >= (SELECT max(timestamp) FROM numbers)" + \
            "LIMIT 1"

    with db.engine.begin() as conn:
        result = conn.execute(text(query))
        return get_response_time(result)


def get_explain_insert_row():
    query = "EXPLAIN ANALYZE " + \
            "INSERT INTO numbers (number,timestamp) " + \
                          "VALUES (" + \
                          str(100) + "," + \
                          str(int(round(time.time() * 1000))) + ");"

    with db.engine.begin() as conn:
        result = conn.execute(text(query))
        return get_response_time(result)

def get_explain_insert_e_row():
    query = "EXPLAIN ANALYZE " + \
            "INSERT INTO numbers (number,timestamp) " + \
                          "VALUES (" + \
                          str(100) + "," + \
                          str(int(round(time.time() * 1000))) + ");"

    with db.engine.begin() as conn:
        result = conn.execute(text(query))
        return get_response_time(result)



if __name__ == '__main__':
    print('Usual insert')
    times = []
    for i in range(10):
        cur_time = get_explain_insert_row()
        print(i, cur_time)
        times.append(cur_time)

    time.sleep(3)

    print('Encrypted insert')
    e_times = []
    for i in range(10):
        cur_time = get_explain_insert_e_row()
        print(i, cur_time)
        e_times.append(cur_time)

    #print("total times:\n", times)


