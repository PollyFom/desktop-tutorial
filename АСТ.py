import sqlite3
import os
import time
from difflib import SequenceMatcher

# КОНФИГУРАЦИЯ
DB_SOURCE_1 = 'source_hr.db'
DB_SOURCE_2 = 'source_fin.db'
DB_DW = 'data_warehouse.db'


# ФУНКЦИИ ОЧИСТКИ БД
def clean_db_files():
    """Удаляет старые файлы БД для чистого запуска"""
    for f in [DB_SOURCE_1, DB_SOURCE_2, DB_DW]:
        if os.path.exists(f):
            try:
                os.remove(f)
                print(f"Удален старый файл: {f}")
            except PermissionError:
                print(f"Не удалось удалить {f}")
                raise


# НОРМАЛИЗАЦИЯ ИМЕНИ
def normalize_name(name: str) -> str:
    """Приводит ФИО к каноническому виду"""
    if not name:
        return ""

    # Приводим к нижнему регистру и убираем точки
    name = str(name).lower().replace(".", "").strip()

    # Разбиваем на слова и сортируем
    tokens = sorted([t.strip() for t in name.split() if t.strip()])

    return " ".join(tokens)


# ПРОВЕРКА ПОХОЖИХ ИМЕН
def is_similar(name1, name2):
    """Проверяет, похожи ли два имени (с учетом опечаток)"""
    if not name1 or not name2:
        return False

    norm1 = normalize_name(name1)
    norm2 = normalize_name(name2)

    if norm1 == norm2:
        return True

    similarity = SequenceMatcher(None, norm1, norm2).ratio()
    return similarity >= 0.7


# СОЗДАНИЕ ИСТОЧНИКА 1 (HR)
def create_source_1():
    conn = sqlite3.connect(DB_SOURCE_1)
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            full_name TEXT NOT NULL
        );
        CREATE TABLE projects (
            id INTEGER PRIMARY KEY,
            name TEXT,
            stack TEXT
        );
        CREATE TABLE roles (
            id INTEGER PRIMARY KEY,
            employee_id INTEGER,
            project_id INTEGER,
            role_name TEXT,
            FOREIGN KEY(employee_id) REFERENCES employees(id),
            FOREIGN KEY(project_id) REFERENCES projects(id)
        );
    """)

    employees = [
        (1, "Иванов Иван Иванович"),
        (2, "ПЕТРОВ ПЕТР"),
        (3, "Быков Алексей"),
        (4, "Kuznetsova Olga")
    ]
    cursor.executemany("INSERT INTO employees VALUES (?, ?)", employees)

    projects = [
        (1, "Alpha Bank App", "Java, Spring, Oracle"),
        (2, "Data Pipeline", "Python, Airflow, PostgreSQL")
    ]
    cursor.executemany("INSERT INTO projects VALUES (?, ?, ?)", projects)

    roles = [
        (1, 1, 1, "Backend Developer"),
        (2, 2, 1, "Team Lead"),
        (3, 3, 2, "Data Engineer"),
        (4, 4, 2, "Analyst")
    ]
    cursor.executemany("INSERT INTO roles VALUES (?, ?, ?, ?)", roles)

    conn.commit()
    conn.close()
    print(f"БД {DB_SOURCE_1} создана")


# СОЗДАНИЕ ИСТОЧНИКА 2 (FIN)
def create_source_2():
    conn = sqlite3.connect(DB_SOURCE_2)
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            full_name TEXT NOT NULL
        );
        CREATE TABLE skills (
            id INTEGER PRIMARY KEY,
            employee_id INTEGER,
            skill_name TEXT,
            FOREIGN KEY(employee_id) REFERENCES employees(id)
        );
        CREATE TABLE salary (
            id INTEGER PRIMARY KEY,
            employee_id INTEGER,
            base_salary REAL,
            bonus REAL,
            FOREIGN KEY(employee_id) REFERENCES employees(id)
        );
    """)

    employees = [
        (1, "иван иванович иванов"),
        (2, "Petrov Petr"),
        (3, "Алксей Быков"),
        (4, "olga kuznetsova"),
        (5, "Иванов И.")
    ]
    cursor.executemany("INSERT INTO employees VALUES (?, ?)", employees)

    skills = [
        (1, 1, "Python"),
        (2, 1, "SQL"),
        (3, 2, "Management"),
        (4, 3, "ETL"),
        (5, 4, "Analysis"),
        (6, 5, "Python"),
        (7, 5, "Java")
    ]
    cursor.executemany("INSERT INTO skills VALUES (?, ?, ?)", skills)

    salary = [
        (1, 1, 200000.0, 50000.0),
        (2, 2, 350000.0, 100000.0),
        (3, 3, 250000.0, 30000.0),
        (4, 4, 180000.0, 20000.0),
        (5, 5, 205000.0, 48000.0)
    ]
    cursor.executemany("INSERT INTO salary VALUES (?, ?, ?, ?)", salary)

    conn.commit()
    conn.close()
    print(f"БД {DB_SOURCE_2} создана")


# СОЗДАНИЕ ХРАНИЛИЩА
def create_dw():
    conn = sqlite3.connect(DB_DW)
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE dim_employee (
            dw_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name_signature TEXT UNIQUE,
            main_name TEXT,
            all_variants TEXT
        );

        CREATE TABLE fact_employee_profile (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dw_employee_id INTEGER UNIQUE,
            role TEXT,
            project_name TEXT,
            tech_stack TEXT,
            skills_list TEXT,
            total_income REAL,
            FOREIGN KEY(dw_employee_id) REFERENCES dim_employee(dw_id)
        );
    """)
    conn.commit()
    conn.close()
    print(f"БД {DB_DW} создана")


# ГЛАВНЫЙ ETL ПРОЦЕСС
def etl_process():
    print("\n--- ЗАПУСК ETL ---")

    conn1 = sqlite3.connect(DB_SOURCE_1)
    conn2 = sqlite3.connect(DB_SOURCE_2)
    conn_dw = sqlite3.connect(DB_DW)

    cur1 = conn1.cursor()
    cur2 = conn2.cursor()
    cur_dw = conn_dw.cursor()

    # Загружаем данные из HR
    cur1.execute("""
        SELECT e.id, e.full_name, r.role_name, p.name, p.stack
        FROM employees e
        JOIN roles r ON e.id = r.employee_id
        JOIN projects p ON r.project_id = p.id
    """)
    hr_data = cur1.fetchall()

    # Загружаем данные из FIN
    cur2.execute("SELECT employee_id, GROUP_CONCAT(skill_name, ', ') FROM skills GROUP BY employee_id")
    skills_map = dict(cur2.fetchall())

    cur2.execute("""
        SELECT e.id, e.full_name, s.base_salary, s.bonus 
        FROM employees e 
        JOIN salary s ON e.id = s.employee_id
    """)
    fin_data = cur2.fetchall()

    # Словарь для уникальных сотрудников
    # Ключ - каноническая сигнатура, значение - данные сотрудника
    employees = {}

    # Сначала добавляем всех из HR
    print("\nОбработка HR данных:")
    for emp_id, name, role, project, stack in hr_data:
        sig = normalize_name(name)

        if sig not in employees:
            employees[sig] = {
                'main_name': name,
                'variants': {name},
                'role': role,
                'project': project,
                'stack': stack,
                'skills': None,
                'income': 0
            }
            print(f"  Добавлен: {name} -> {sig}")
        else:
            # Если уже есть такой сотрудник (дубликат в HR)
            employees[sig]['variants'].add(name)

    # Обрабатываем FIN и ищем соответствия
    print("\nОбработка FIN данных (поиск опечаток):")
    for emp_id, name, salary, bonus in fin_data:
        sig = normalize_name(name)
        skills = skills_map.get(emp_id, "Нет данных")
        income = salary + bonus

        # Ищем похожую сигнатуру среди существующих сотрудников
        found_sig = None

        # Сначала точное совпадение
        if sig in employees:
            found_sig = sig
        else:
            # Поиск похожих (для опечаток)
            for existing_sig in employees.keys():
                if is_similar(sig, existing_sig):
                    found_sig = existing_sig
                    print(f"  → Опечатка: '{name}' → соответствует '{employees[found_sig]['main_name']}'")
                    break

        if found_sig:
            # Объединяем с существующим сотрудником
            employees[found_sig]['variants'].add(name)
            employees[found_sig]['skills'] = skills
            employees[found_sig]['income'] = income
        else:
            # Новый сотрудник (только из FIN)
            print(f"  → Новый сотрудник (только FIN): {name}")
            employees[sig] = {
                'main_name': name,
                'variants': {name},
                'role': None,
                'project': None,
                'stack': None,
                'skills': skills,
                'income': income
            }

    # Загружаем в хранилище
    print("\nЗагрузка в хранилище:")
    for sig, emp in employees.items():
        # Объединяем все варианты имен в строку
        variants_str = ", ".join(sorted(emp['variants']))

        # Добавляем в измерение
        cur_dw.execute("""
            INSERT INTO dim_employee (name_signature, main_name, all_variants)
            VALUES (?, ?, ?)
        """, (sig, emp['main_name'], variants_str))

        dw_id = cur_dw.lastrowid

        # Добавляем в факты
        cur_dw.execute("""
            INSERT INTO fact_employee_profile 
            (dw_employee_id, role, project_name, tech_stack, skills_list, total_income)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (dw_id, emp['role'], emp['project'], emp['stack'],
              emp['skills'], emp['income']))

        print(f"  Загружен: {emp['main_name']} (доход: {emp['income']:.2f})")

    conn_dw.commit()

    # Выводим результаты
    print("\n" + "=" * 140)
    print("ИТОГОВАЯ БАЗА ДАННЫХ (КАЖДЫЙ СОТРУДНИК - 1 РАЗ)")
    print("=" * 140)

    cur_dw.execute("""
        SELECT d.name_signature, d.main_name, d.all_variants,
               f.role, f.project_name, f.skills_list, f.total_income
        FROM dim_employee d
        JOIN fact_employee_profile f ON d.dw_id = f.dw_employee_id
        ORDER BY d.main_name
    """)

    print(f"\n{'№':<3} | {'Основное имя':<30} | {'Роль':<20} | {'Проект':<20} | {'Доход':<12}")
    print("-" * 90)

    for i, row in enumerate(cur_dw.fetchall(), 1):
        sig, main_name, variants, role, project, skills, income = row
        role_str = str(role) if role else "Нет данных"
        project_str = str(project) if project else "Нет данных"

        print(f"{i:<3} | {main_name[:30]:<30} | {role_str[:20]:<20} | {project_str[:20]:<20} | {income:<12.2f}")

        # Показываем все варианты имен
        if variants and ',' in variants:
            print(f"    Варианты имен: {variants}")


    # Проверяем, нет ли дубликатов
    cur_dw.execute("SELECT COUNT(*) FROM dim_employee")
    dim_count = cur_dw.fetchone()[0]
    cur_dw.execute("SELECT COUNT(*) FROM fact_employee_profile")
    fact_count = cur_dw.fetchone()[0]


    conn1.close()
    conn2.close()
    conn_dw.close()


# ЗАПУСК
if __name__ == "__main__":
    try:
        clean_db_files()
        create_source_1()
        create_source_2()
        create_dw()
        etl_process()
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        raise