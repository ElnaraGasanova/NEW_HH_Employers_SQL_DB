from utils.utils import get_employers_data, get_all_vacancies, create_table, fill_in_table
from src.db_manager import DBManager

import os

PG_KEY: str = os.getenv('POSTGRESSQL_KEY')


def main():
    """Код для проверки работоспособности программы"""
    # Создаем список id компаний с НН
    employer_ids = [4649269, 2733062, 15478, 42481, 907345, 4934, 2624085, 1373, 23427, 4023]
    data_all = []
    emp_list = get_employers_data(employer_ids)
    vacancies_list = get_all_vacancies(employer_ids)
    data_all.append({
        "employers": emp_list,
        "vacancies": vacancies_list
    })

    create_table()
    fill_in_table(data_all)

    print("Приветствую! Предлагаю ознакомиться с вакансиями ведущих работодателей Москвы!\n")

    db_manager = DBManager()

    while True:

        task = input(
            "Получить список всех доступных компаний и количество вакансий у каждой компании, введите: 1\n"
            "Получить список всех вакансий с указанием названия компании, вакансии, зарплаты "
            "и ссылки на вакансию, введите: 2\n"
            "Получить среднюю зарплату по вакансиям, введите: 3\n"
            "Получить список всех вакансий, где зарплата выше средней по всем вакансиям, введите: 4\n"
            "Получить список всех вакансий по вашему запросу, введите: 5\n"
            "Завершить работу, введите: стоп\n"
        )

        if task == 'стоп':
            break
        elif task == '1':
            print(db_manager.get_companies_and_vacancies_count())
            print()
        elif task == '2':
            print(db_manager.get_all_vacancies())
            print()
        elif task == '3':
            print(db_manager.get_avg_salary())
            print()
        elif task == '4':
            print(db_manager.get_vacancies_with_higher_salary())
            print()
        elif task == '5':
            keyword = input('Введите ключевое слово поиска вакансии: ')
            print(db_manager.get_vacancies_with_keyword(keyword))
            print()
        else:
            print('Неправильный запрос')


if __name__ == "__main__":
    main()
