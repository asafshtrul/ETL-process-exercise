import MySQLdb
import csv
import requests
import json
from datetime import datetime

URL = "http://api.open-notify.org/iss-pass.json"
CSV_NAME = 'avg.csv'


def get_iss_passes(lat, lon):
    params_dict = {
        "n": 50,
        "lat": lat,
        "lon": lon
    }

    response = requests.get(URL, params=params_dict)
    resp_object = json.loads(response.text)
    return resp_object["response"]


def open_db_connection(host_name, user_name, password, db):
    return MySQLdb.connect(host_name, user_name, password, db)


def run_db(cmd, my_db):

    cursor = my_db.cursor()

    try:
        cursor.execute(cmd)
        my_db.commit()
    except Exception as ex:
        print(ex)
        my_db.rollback()

    rows = cursor.fetchall()
    return rows


def run_db_no_transact(cmd, my_db):

    cursor = my_db.cursor()

    try:
        cursor.execute(cmd)
    except Exception as ex:
        print(ex)

    rows = cursor.fetchall()
    return rows


def insert_passes_to_db(passes, city, db):
    for iss_pass in passes:
        duration = iss_pass["duration"]
        risetime = datetime.utcfromtimestamp(iss_pass["risetime"]).strftime('%Y-%m-%d %H:%M:%S')
        query = f"""INSERT INTO orbital_data_asaf_shtrul (city, risetime, duration) VALUES ("{city}", "{risetime}", {duration}); """
        run_db(query, db)


def get_pass_avg(query, file_name, db):
    rows = run_db_no_transact(query, db)
    with open(file_name, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',')
        for row in rows:
            csv_writer.writerow(row)


def main():
    with open("connect.json", "r") as details_file:
        details = json.loads(details_file.read())
        host = details["host_name"]
        user = details["user_name"]
        password = details["password"]
        database = details["database_name"]

    my_db = open_db_connection(host, user, password, database)

    with open("cities.json", "r") as cities_file:
        cities = json.loads(cities_file.read())["cities"]
        for city in cities:
            passes = get_iss_passes(city["lat"], city["lon"])
            insert_passes_to_db(passes, city["name"], my_db)
            get_pass_avg("call interview.city_stats_asaf_shtrul();", CSV_NAME, my_db)
    my_db.close()


if __name__ == '__main__':
    main()



