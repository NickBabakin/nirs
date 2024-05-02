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


def explain_insert(phone_number: str):
    call_text = f"text that happened during call {phone_number}"
    query = (
        f"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)"
        f"INSERT INTO calls (phone_number, call_text) "
        f"VALUES ('{phone_number}', '{call_text}')"
    )
    with db.engine.begin() as conn:
        result = conn.execute(text(query))
        return get_response_time(result)


def explain_e_insert(phone_number: str):
    call_text = f"text that happened during call {phone_number}"
    query = (
        f"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)"
        f"INSERT INTO calls (phone_number, call_text) "
        f"VALUES ('{phone_number}', pgp_sym_encrypt('{call_text}', '{key}', 'compress-algo=1, cipher-algo=aes256'))"
    )
    with db.engine.begin() as conn:
        result = conn.execute(text(query))
        return get_response_time(result)


def delete_from_calls():
    query = (
        f"DELETE FROM calls; "
    )
    with db.engine.begin() as conn:
        conn.execute(text(query))
        return


if __name__ == '__main__':
    fun_name = sys.argv[1]
    fun = None
    delete_from_calls()
    ids = range(10000)
    if fun_name == "ins":
        fun = explain_insert
    elif fun_name == "eins":
        fun = explain_e_insert
    l = [[fun(str(i))] for i in ids]
    df = pd.DataFrame(l, columns=['time_ms'])

    time_ms = df.mean().iloc[0]
    print(f"{fun_name:} mean time is \n {time_ms} ms")
