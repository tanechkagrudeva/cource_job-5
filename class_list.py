import psycopg2


class DBManager:
    def __init__(self):
        self.conn = psycopg2.connect(dbname='test', user='postgres', password='Zima-2023', host='localhost')


    def all_table_select(self):
        """создаем таблицы с информацией о компаниях и вакансиях"""

        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                city TEXT NOT NULL,
                description TEXT,
                url TEXT NOT NULL
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vacancies (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                company_id INTEGER REFERENCES companies(id),
                salary_min INTEGER,
                salary_max INTEGER,
                url TEXT NOT NULL
            );                
        """)
        self.conn.commit()

    def include_table(self, company, vacancies):
        cur = self.conn.cursor()
        cur.execute(f"""
                    INSERT INTO companies (id, name, city, description, url)
                    VALUES (%s, %s, %s, %s, %s)""", (int(company['id']), company['name'],
                                              company['area']['name'], company['description'],
                                              company['alternate_url'])
                    )
        self.conn.cursor()

        for i in vacancies['items']:
            from_s = None
            to_s = None
            if i['salary']:
                from_s = i['salary']['from']
                to_s = i['salary']['to']
            cur.execute(f"""
                        INSERT INTO vacancies (name, company_id, salary_min, salary_max, url)
                        VALUES (%s, %s, %s, %s, %s)""",
                        (i['name'], i['employer']['id'], from_s, to_s, i['alternate_url']
                        )
            )
            self.conn.commit()


    def drop_all_tables(self):
        """удаляет таблицы"""
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXIST vacancies;")
        cur.execute("DROP TABLE IF EXIST companies;")

    def get_companies_and_vacancies_count(self):
        """получает список всех компаний и количество вакансий у каждой компании"""

        cur = self.conn.cursor()
        cur.execute("""
            SELECT companies.name, COUNT(vacancies.id)
            FROM companies JOIN vacancies ON vacancies.company_id = companies.id
            GROUP BY companies.name;
        """)
        return cur.fetchall()

    def get_all_vacancies(self):
        """получает список всех вакансий с указанием названия компании, названия вакансии и
    зарплаты и ссылки на вакансию."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT companies.name, vacancies.name, vacancies.salary_min, vacancies.salary_max, vacancies.url
            FROM vacancies JOIN companies ON vacancies.company_id = companies.id;
        """)
        return cur.fetchall()


    def get_avg_salary(self):
        """получает среднюю зарплату по вакансиям."""

        cur = self.conn.cursor()
        cur.execute("""
            SELECT AVG(vacancies.salary_max)
            FROM vacancies;
        """)
        return cur.fetchone()

    def get_vacancies_with_higher_salary(self):
        """получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""

        cur = self.conn.cursor()
        cur.execute(f"""
            SELECT companies.name, vacancies.name, vacancies.salary_min, vacancies.salary_max, vacancies.url
            FROM vacancies JOIN companies ON vacancies.company_id = companies.id
            WHERE vacancies.salary_max > ({self.get_avg_salary()[0]});
        """)
        return cur.fetchall()


    def get_vacancies_with_keyword(self, keyword):
        """получает список всех вакансий, в названии которых содержатся переданные в метод слова, например 'python'"""

        cur = self.conn.cursor()
        cur.execute(f"""
            SELECT *
            FROM vacancies WHERE vacancies.name ILIKE '%{keyword}%';
        """)
        return cur.fetchall()

if __name__ == '__main__':
    dbm = DBManager()
