import os
import requests
import psycopg2
from typing import Any

PG_KEY = os.getenv('POSTGRESSQL_KEY')


def get_employers_data(employer_ids: list) -> list[dict]:
    """Получение данных c HeadHunter.ru о работодателе."""

    emp_list = []
    for employer_id in employer_ids:
        url = f"https://api.hh.ru/employers/{employer_id}"

        params = {
            "employer_id": employer_id,  # поиск по id работодателя.
        }
        response = requests.get(url, params=params)
        if response.ok:
            data = response.json()
            employer_dict = {
                "name": data["name"],
                "external_id": data["id"],
                "url_hh": data["alternate_url"],
                "site_url": data["site_url"],
                "description": data["description"]
            }
            emp_list.append(employer_dict)
        elif employer_ids is None:
            print("Данные не получены.")
        elif 'items' not in employer_ids:
            print("Указанный работодатель не найден.")
            quit()
    return emp_list


def get_all_vacancies(employer_ids: list) -> list[dict]:
    """Получение данных c HeadHunter.ru о вакансиях работодателя."""
    vacancies_list = []
    for employer_id in employer_ids:
        url = "https://api.hh.ru/vacancies"
        params = {
            "per_page": 20,     # количество вакансий на странице.
            "page": None,  # номер страницы результатов.
            "employer_id": employer_id,  # строка поиска по названию вакансии.
            "area": 1,  # Код региона (1 - Москва)
        }
        response = requests.get(url, params=params)
        if response.ok:
            data = response.json()
            for vacancy in data["items"]:
                vacancy_info = {
                    "employer": vacancy["employer"].get("name"),
                    "employer_id": vacancy["employer"].get("id"),
                    "employer_url": vacancy["employer"].get("alternate_url"),
                    "title": vacancy.get("name"),
                    "vacancy_id": vacancy.get("id"),
                    "url": vacancy.get("apply_alternate_url"),
                    "salary_from": vacancy["salary"].get("from") if vacancy["salary"] else None,
                    "salary_to": vacancy["salary"].get("to") if vacancy["salary"] else None,
                    "description": vacancy.get("snippet", {}).get("requirement")
                }
                if vacancy_info["salary_from"] is None:
                    vacancy_info["salary_from"] = 0

                if vacancy_info["salary_to"] is None:
                    vacancy_info["salary_to"] = 0

                if vacancy_info["salary_to"] == 0:
                    vacancy_info["salary_to"] = vacancy_info["salary_from"]

                vacancies_list.append(vacancy_info)
        else:
            print("Запрос не выполнен")
            quit()
    return vacancies_list


def create_table() -> None:
    """SQL. Создание БД и таблиц."""
    conn = psycopg2.connect(host="localhost", database="postgres",
                            user="postgres", password=PG_KEY, client_encoding="UTF-8")
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP DATABASE IF EXISTS hh_employers_db")
    cur.execute("CREATE DATABASE hh_employers_db")

    cur.close()
    conn.close()

    conn = psycopg2.connect(host="localhost", database="hh_employers_db",
                            user="postgres", password=PG_KEY, client_encoding="UTF-8")
    with conn.cursor() as cur:
        cur.execute(f"""CREATE TABLE employers
                      (employer_id SERIAL PRIMARY KEY,
                    employer_name VARCHAR(50) NOT NULL,
                    external_id INTEGER NOT NULL,
                    site_url VARCHAR(200),
                    description TEXT)""")

        cur.execute(f"""CREATE TABLE vacancies
                      (vacancy_id SERIAL PRIMARY KEY,
                      employer_id INTEGER REFERENCES employers(employer_id),
                      external_id INTEGER NOT NULL,
                      vacancy_title VARCHAR NOT NULL,
                      vacancy_url VARCHAR(200),
                      salary_from DECIMAL,
                      salary_to DECIMAL,
                      description TEXT)""")

    cur.close()
    conn.commit()
    conn.close()


def fill_in_table(data: list[dict[str, Any]]) -> None:
    """SQL. Заполнение таблиц данными."""
    with psycopg2.connect(host="localhost", database="hh_employers_db",
                          user="postgres", password=PG_KEY) as conn:
        with conn.cursor() as cur:
            #cur.execute('TRUNCATE TABLE employers, vacancies RESTART IDENTITY;')

            for employer in data[0]['employers']:
                #employer_list = employer['employer']
                cur.execute('INSERT INTO employers (employer_name, external_id, site_url, description) '
                            'VALUES (%s, %s, %s, %s) RETURNING employer_id',
                            (employer['name'], employer['external_id'], employer['site_url'],
                             employer['description'])
                            )
                employer_id = cur.fetchone()[0]
                employer_name = employer['name']
                for vacancy in data[0]['vacancies']:
                    if employer_name == vacancy['employer']:
                        cur.execute('INSERT INTO vacancies (vacancy_title, employer_id, external_id,'
                                    'vacancy_url, salary_from, salary_to, description) '
                                    'VALUES (%s, %s, %s, %s, %s, %s, %s)',
                                    (vacancy['title'], employer_id, vacancy['vacancy_id'],
                                     f"https://hh.ru/applicant/vacancy_response?vacancyId={vacancy['vacancy_id']}",
                                     vacancy['salary_from'], vacancy['salary_to'], vacancy['description']))

    conn.commit()
    conn.close()