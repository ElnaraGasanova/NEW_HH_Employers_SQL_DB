import psycopg2
import os

PG_KEY = os.getenv('POSTGRESSQL_KEY')


class DBManager:
    """Класс подключения и работы к БД PostgreSQL"""

    def __init__(self) -> None:
        """Инициализация Класса DBManager"""
        self.connection = None
        self.cur = None

    def connect(self):
        """Подключение к базе данных"""
        self.connection = psycopg2.connect(host="localhost", database="hh_employers_db",
                                           user="postgres", password=PG_KEY)
        print("Подключение к Базе данных\n")

    def disconnect(self):
        """Отключение от базы данных"""
        self.connection.close()
        print("Отключение от Базы данных.")

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний
        и количество вакансий у каждой компании."""
        self.cur = self.connection.cursor()
        self.cur.execute(f"SELECT employer_name, COUNT(*) AS count_vacancies "
                         f"FROM vacancies "
                         f"INNER JOIN employers USING(employer_id) "
                         f"GROUP BY employer_name")
        results = self.cur.fetchall()
        for result in results:
            print(*result)
        self.cur.close()

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием
        названия компании, названия вакансии и зарплаты
        и ссылки на вакансию."""
        self.cur = self.connection.cursor()
        self.cur.execute(f"SELECT employer_name, vacancy_title, salary_from, salary_to, vacancy_url "
                         f"FROM vacancies "
                         f"INNER JOIN employers USING (employer_id)")
        results = self.cur.fetchall()
        for result in results:
            print(*result)
        self.cur.close()

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям."""
        self.cur = self.connection.cursor()
        self.cur.execute(f"SELECT AVG(salary_from) FROM vacancies ")
        results = self.cur.fetchone()
        result = int(results[0])
        print(f"Cредний размер зарплаты по всем вакансиям составляет {result} рублей")
        self.cur.close()

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых
        зарплата выше средней по всем вакансиям."""
        self.cur = self.connection.cursor()
        self.cur.execute('''SELECT employer_name, vacancy_title, salary_from, salary_to, vacancy_url
        FROM vacancies
        INNER JOIN employers USING (employer_id)
        WHERE salary_from > ANY (SELECT AVG(salary_from) FROM vacancies)
        ORDER BY salary_from DESC''')
        results = self.cur.fetchall()
        for result in results:
            print(*result)
        self.cur.close()

    def get_vacancies_with_keyword(self, keyword):
        """Получает список всех вакансий, в названии которых
        содержатся переданные в метод слова, например python."""
        self.cur = self.connection.cursor()
        self.cur.execute(f"SELECT employer_name, vacancy_title, vacancy_url, salary_from, salary_to "
                         f"FROM vacancies "
                         f"INNER JOIN employers USING(employer_id)"
                         f"WHERE lower(vacancy_title) ILIKE '%{keyword}%' "
                         )
        results = self.cur.fetchall()
        if len(results) == 0:
            print("Вакансий не найдено.")
        for result in results:
            print(*result)
        self.cur.close()


def format_salary(salary_from, salary_to):
    if salary_from is None:
        salary_from = 'не указано'
    if salary_to is None:
        salary_to = 'не указано'
    return salary_from, salary_to
