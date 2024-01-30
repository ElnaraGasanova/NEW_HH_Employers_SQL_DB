import psycopg2
import os

PG_KEY=os.getenv('POSTGRESSQL_KEY')


class DBManager:
    """Класс подключения и работы к БД PostgreSQL"""

    @staticmethod
    def get_companies_and_vacancies_count():
        """Получает список всех компаний
        и количество вакансий у каждой компании."""
        with psycopg2.connect(host="localhost", database="hh_employers_db",
                              user="postgres", password=PG_KEY) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT employer_name, COUNT(*) AS count_vacancies "
                            f"FROM vacancies "
                            f"INNER JOIN employers USING(employer_id) "
                            f"GROUP BY employer_name")
                results = cur.fetchall()
                for result in results:
                    print(*result)

    @staticmethod
    def get_all_vacancies():
        """Получает список всех вакансий с указанием
        названия компании, названия вакансии и зарплаты
        и ссылки на вакансию."""
        with psycopg2.connect(host="localhost", database="hh_employers_db",
                              user="postgres", password=PG_KEY) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT employer_name, vacancy_title, salary_from, salary_to, vacancy_url "
                            f"FROM vacancies "
                            f"INNER JOIN employers USING (employer_id)")
                results = cur.fetchall()
                for result in results:
                    print(*result)

    @staticmethod
    def get_avg_salary():
        """Получает среднюю зарплату по вакансиям."""
        with psycopg2.connect(host="localhost", database="hh_employers_db",
                              user="postgres", password=PG_KEY) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT AVG(salary_from) FROM vacancies ")
                results = cur.fetchone()
                result = int(results[0])
                print(f"Cредний размер зарплаты по всем вакансиям составляет {result} рублей")

    @staticmethod
    def get_vacancies_with_higher_salary():
        """Получает список всех вакансий, у которых
        зарплата выше средней по всем вакансиям."""
        with psycopg2.connect(host="localhost", database="hh_employers_db",
                              user="postgres", password=PG_KEY) as conn:
            with conn.cursor() as cur:
                cur.execute('''SELECT employer_name, vacancy_title, salary_from, salary_to, vacancy_url
                FROM vacancies
                INNER JOIN employers USING (employer_id)
                WHERE salary_from > ANY (SELECT AVG(salary_from) FROM vacancies)
                ORDER BY salary_from DESC''')
                results = cur.fetchall()
                for result in results:
                    print(*result)

    @staticmethod
    def get_vacancies_with_keyword(keyword):
        """Получает список всех вакансий, в названии которых
        содержатся переданные в метод слова, например python."""
        with psycopg2.connect(host="localhost", database="hh_employers_db",
                              user="postgres", password=PG_KEY) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT employer_name, vacancy_title, vacancy_url, salary_from, salary_to "
                            f"FROM vacancies "
                            f"INNER JOIN employers USING(employer_id)"
                            f"WHERE lower(vacancy_title) ILIKE '%{keyword}%' "
                            )
                results = cur.fetchall()
                if len(results) == 0:
                    print("Вакансий не найдено.")
                for result in results:
                    print(*result)



def format_salary(salary_from, salary_to):
    if salary_from is None:
        salary_from = 'не указано'
    if salary_to is None:
        salary_to = 'не указано'
    return salary_from, salary_to
