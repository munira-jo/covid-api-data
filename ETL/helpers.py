from datetime import date, datetime, timedelta
from dotenv import dotenv_values, find_dotenv
from flatten_dict import flatten
from psycopg2.errorcodes import UNIQUE_VIOLATION
from psycopg2 import connect, errors
import requests

config = dotenv_values(find_dotenv())


def extract_data_from_api(
    date, endpoint_stub="https://api.covidtracking.com/v2/us/daily/"
) -> dict:
    """
    Extracts data for the given date from the endpoint.
    """
    endpoint_full = endpoint_stub + str(date) + "/simple.json"
    response = requests.get(endpoint_full).json()
    if "data" in response.keys():
        return response["data"]
    else:
        return dict()


def flatten_response(response_from_api) -> dict:
    """
    Flattens the nested json response from the API.
    """
    return flatten(response_from_api, reducer="underscore")


def get_connection_to_database():
    """
    Connects to the database.
    """
    conn = connect(
        database=config["POSTGRES_DB"],
        user="postgres",
        password=config["POSTGRES_PASSWORD"],
        host="db",
        port="5432",
    )
    print("Connected to database!")
    return conn


def get_base_stats_table_data_from_api_response(api_response) -> set:
    """
    Gets the relevant data for the base_stats table from the API response
    """
    api_response = flatten_response(api_response)
    base_stats_data = (
        api_response["date"],
        api_response["states"],
        api_response["cases_total"],
        api_response["testing_total"],
    )
    return base_stats_data


def write_to_base_stats_table(base_stats_row) -> None:
    """
    Creates the base_stats table if not doesn't already exist. Then it inserts the relevant
    data from the API into the table.
    """
    with get_connection_to_database() as db_connection:
        cursor = db_connection.cursor()
        create_sql = """CREATE TABLE IF NOT EXISTS base_stats (base_id serial PRIMARY KEY, date date, states int, total_cases bigint, total_tested bigint, UNIQUE (date));"""
        cursor.execute(create_sql)
        try:
            insert_sql = """INSERT INTO base_stats (date, states,total_cases,total_tested) VALUES (%s, %s, %s, %s) """
            cursor.execute(insert_sql, base_stats_row)
            db_connection.commit()
        except errors.lookup(UNIQUE_VIOLATION) as e:
            print("Date already exists in table base_stats")
            pass


def get_outcome_stats_table_data_from_api_response(api_response) -> set:
    """
    Gets the relevant data for the outcome_stats table from the API response
    """
    api_response = flatten_response(api_response)
    outcome_stats_data = (
        api_response["outcomes_hospitalized_currently"],
        api_response["outcomes_hospitalized_in_icu_currently"],
        api_response["outcomes_hospitalized_on_ventilator_currently"],
        api_response["outcomes_death_total"],
        api_response["date"],
    )
    return outcome_stats_data


def write_to_outcome_stats_table(outcome_stats_row) -> None:
    """
    Creates the outcome_stats table if not doesn't already exist. Then it inserts the relevant
    data from the API into the table.
    """
    with get_connection_to_database() as db_connection:
        cursor = db_connection.cursor()
        create_sql = """CREATE TABLE IF NOT EXISTS outcome_stats (outcome_id serial PRIMARY KEY, total_hospitalized bigint, total_hospitalized_in_icu bigint, total_hospitalized_on_ventilator bigint, total_deaths bigint, base_id int);"""
        cursor.execute(create_sql)
        insert_sql = """INSERT INTO outcome_stats (total_hospitalized, total_hospitalized_in_icu,total_hospitalized_on_ventilator,total_deaths, base_id) VALUES ( %s, %s, %s, %s, (SELECT base_id FROM base_stats where date=%s)) """
        cursor.execute(insert_sql, outcome_stats_row)
        db_connection.commit()


def extract_data_from_api_and_load_to_database(date) -> None:
    """
    Extracts the data for the given date from the API and writes it
    the tables base_stats and outcome_stats on the database
    """

    response = extract_data_from_api(date)
    base_stats_row = get_base_stats_table_data_from_api_response(response)
    write_to_base_stats_table(base_stats_row)
    outcome_stats_row = get_outcome_stats_table_data_from_api_response(response)
    write_to_outcome_stats_table(outcome_stats_row)


def backfill_data(start_date, end_date) -> None:
    """
    Backfill data for the period from start_date to end_date. Get the data
    from the API for each day in this period and write to database.
    """
    date1 = datetime.strptime(start_date, "%Y-%m-%d")
    date2 = datetime.strptime(end_date, "%Y-%m-%d")
    list_of_dates = [date1 + timedelta(days=x) for x in range((date2 - date1).days + 1)]
    list_of_dates = [day.strftime("%Y-%m-%d") for day in list_of_dates]
    for date_to_backfill in list_of_dates:
        print(f"Backfilling for {date_to_backfill}")
        extract_data_from_api_and_load_to_database(date_to_backfill)
