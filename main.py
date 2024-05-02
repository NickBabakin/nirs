import time
import random
import psycopg2
import sys

from sqlalchemy import create_engine, text
import pandas as pd

db_name = 'db'
db_user = 'user'
db_pass = 'passw'
db_host = 'localhost'
db_port = '5432'

# Connect to the database
db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)

key = "top_secret"


def parse_ms(s: str):
    if s[-2:] != 'ms':
        print(f"ERR: {s[-2:]} is not ms")
    return float(s[-8:-3])


def get_response_time(result):
    second_to_last = ""
    last = ""
    for row in result:  # iterating to get last row of answer
        second_to_last = last
        last = row[0]
    return parse_ms(second_to_last) + parse_ms(last)


def explain_insert(username: str):
    password = f"passw{username}"
    query = (
        f"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)"
        f"INSERT INTO users (username, password) "
        f"VALUES ('{username}', '{password}')"
    )
    with db.engine.begin() as conn:
        result = conn.execute(text(query))
        return get_response_time(result)


def explain_e_insert(username: str):
    password = f"passw{username}"
    query = (
        f"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)"
        f"INSERT INTO users (username, password) "
        f"VALUES ('{username}', pgp_sym_encrypt('{password}', '{key}', 'compress-algo=1, cipher-algo=aes256'))"
    )
    with db.engine.begin() as conn:
        result = conn.execute(text(query))
        return get_response_time(result)


def explain_e_select():
    query = (
        f"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)"
        f"SELECT username, pgp_sym_decrypt(password::bytea, '{key}') as password "
        f"FROM users where username = 'oleg_e';"
    )
    with db.engine.begin() as conn:
        result = conn.execute(text(query))
        return get_response_time(result)


def e_select():
    query = (
        f"SELECT username, pgp_sym_decrypt(password::bytea, '{key}') as password "
        f"FROM users where username = 'oleg_e';"
    )
    with db.engine.begin() as conn:
        result = conn.execute(text(query))
        for row in result:
            print(row)


def delete_from_users():
    query = (
        f"DELETE FROM users; "
    )
    with db.engine.begin() as conn:
        conn.execute(text(query))
        return


if __name__ == '__main__':
    fun_name = sys.argv[1]
    fun = None
    delete_from_users()
    ids = range(10000)
    if fun_name == "ins":
        fun = explain_insert
    elif fun_name == "eins":
        fun = explain_e_insert
    l = [[fun(str(i))] for i in ids]
    df = pd.DataFrame(l, columns=['time_ms'])

    time_ms = df.mean().iloc[0]
    print(f"{fun_name:} mean time is \n {time_ms} ms")
