import requests
from class_list import DBManager


def get_request(url):
    data = requests.get(url)
    return data.json()


def main():
    dbm = DBManager()
    dbm.all_table_select()
    employees_id = [3672566, 1497835, 4300631, 6093775, 9514501, 4949, 856498, 6120481, 68587, 3863077]

    for employer in employees_id:
        company = get_request(f'https://api.hh.ru/employers/{employer}')
        vacancies = get_request(f'https://api.hh.ru/vacancies?employer_id={employer}&per_page=80')
        dbm.include_table(company, vacancies)

    input('Нажмите Enter')
    print(dbm.get_companies_and_vacancies_count())
    input('Нажмите Enter')
    print(dbm.get_all_vacancies())
    input('Нажмите Enter')
    print(dbm.get_avg_salary())
    input('Нажмите Enter')
    print(dbm.get_vacancies_with_higher_salary())
    name_vacancy = input("Введите интересующую Вас вакансию:")
    print(dbm.get_vacancies_with_keyword(name_vacancy))


if __name__ == '__main__':
    main()
