import sqlite3
import os
import time

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
                print(f" Не удалось удалить {f}. Закройте программу, использующую этот файл.")
                raise

# НОРМАЛИЗАЦИЯ ИМЕНИ
def normalize_name(name: str) -> str:
    """
    Приводит ФИО к каноническому виду.
    ПОНИМАЕТ ИНИЦИАЛЫ! (например, 'а.' = 'алексей')
    """
    if not name:
        return ""
    
    # 1. Словарь расшифровки инициалов
    initials_map = {
        "а": "алексей", "и": "иван", "п": "петр", "с": "сергей",
        "д": "дмитрий", "м": "михаил", "в": "владимир", "н": "николай",
        "е": "елена", "о": "ольга", "т": "татьяна", "к": "константин"
    }
    
    # 2. Приводим к нижнему регистру и убираем точки
    name = str(name).lower().replace(".", "").strip()
    
    # 3. Разбиваем на слова
    tokens = [t.strip() for t in name.split() if t.strip()]
    
    # 4. Заменяем инициалы на полные имена
    cleaned_tokens = []
    for token in tokens:
        if len(token) == 1 and token in initials_map:
            cleaned_tokens.append(initials_map[token])
        else:
            cleaned_tokens.append(token)
            
    # 5. Сортируем и собираем обратно (для надежного сравнения)
    cleaned_tokens.sort()
    return " ".join(cleaned_tokens)

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
    print(f"БД {DB_SOURCE_1} создана и заполнена.")

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
        (1, "иван ИВАНОВИЧ иванов"),
        (2, "Петр Петров"),
        (3, "БЫКОВ А."),            
        (4, "olga kuznetsova")
    ]
    cursor.executemany("INSERT INTO employees VALUES (?, ?)", employees)
    
    skills = [
        (1, 1, "Python"),
        (2, 1, "SQL"),
        (3, 2, "Management"),
        (4, 3, "ETL"),
        (5, 4, "Analysis")
    ]
    cursor.executemany("INSERT INTO skills VALUES (?, ?, ?)", skills)
    
    salary = [
        (1, 1, 200000.0, 50000.0),
        (2, 2, 350000.0, 100000.0),
        (3, 3, 250000.0, 30000.0),
        (4, 4, 180000.0, 20000.0)
    ]
    cursor.executemany("INSERT INTO salary VALUES (?, ?, ?, ?)", salary)
    
    conn.commit()
    conn.close()
    print(f"БД {DB_SOURCE_2} создана и заполнена.")

# --- СОЗДАНИЕ ХД (DW) ---
def create_dw():
    conn = sqlite3.connect(DB_DW)
    cursor = conn.cursor()
    
    cursor.executescript("""
        CREATE TABLE dim_employee (
            dw_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name_signature TEXT UNIQUE,
            name_source_1 TEXT,
            name_source_2 TEXT
        );
        
        CREATE TABLE fact_employee_profile (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dw_employee_id INTEGER,
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
    print(f"БД {DB_DW} создана.")

# ETL ПРОЦЕСС
def etl_process():
    print("\n--- ЗАПУСК ETL ПРОЦЕССА ---")
    
    conn1 = sqlite3.connect(DB_SOURCE_1)
    conn2 = sqlite3.connect(DB_SOURCE_2)
    conn_dw = sqlite3.connect(DB_DW)
    
    cur1 = conn1.cursor()
    cur2 = conn2.cursor()
    cur_dw = conn_dw.cursor()
    
    # Извлечение из Источника 1 (HR)
    query1 = """
        SELECT e.id, e.full_name, r.role_name, p.name, p.stack
        FROM employees e
        JOIN roles r ON e.id = r.employee_id
        JOIN projects p ON r.project_id = p.id
    """
    cur1.execute(query1)
    data_hr = cur1.fetchall()
    
    # Извлечение навыков из Источника 2 (FIN)
    query2_skills = """
        SELECT employee_id, GROUP_CONCAT(skill_name, ', ') 
        FROM skills GROUP BY employee_id
    """
    cur2.execute(query2_skills)
    skills_map = {row[0]: row[1] for row in cur2.fetchall()}
    
    # Извлечение зарплат из Источника 2 (FIN)
    query2_salary = """
        SELECT e.id, e.full_name, s.base_salary, s.bonus 
        FROM employees e 
        JOIN salary s ON e.id = s.employee_id
    """
    cur2.execute(query2_salary)
    data_fin = cur2.fetchall()
    
    # Словарь для объединенных данных
    unified_data = {}
    
    # Обработка Источника 1
    for row in data_hr:
        emp_id, name, role, project, stack = row
        sig = normalize_name(name)
        
        if sig not in unified_data:
            unified_data[sig] = {'names': set(), 'hr': [], 'fin': None}
        
        unified_data[sig]['names'].add(name)
        unified_data[sig]['hr'].append({
            'role': role, 'project': project, 'stack': stack
        })
        
    # Обработка Источника 2
    for row in data_fin:
        emp_id, name, salary, bonus = row
        sig = normalize_name(name)
        skills = skills_map.get(emp_id, "Нет данных")
        total_income = (salary + bonus) if salary and bonus else 0
        
        if sig not in unified_data:
            unified_data[sig] = {'names': set(), 'hr': [], 'fin': None}
            
        unified_data[sig]['names'].add(name)
        unified_data[sig]['fin'] = {
            'skills': skills, 
            'income': total_income
        }
        
    # Загрузка в ХД
    for sig, data in unified_data.items():
        names_list = list(data['names'])
        name_s1 = names_list[0] if len(names_list) > 0 else None
        name_s2 = names_list[-1] if len(names_list) > 1 else None
        
        cur_dw.execute("""
            INSERT INTO dim_employee (name_signature, name_source_1, name_source_2)
            VALUES (?, ?, ?)
        """, (sig, name_s1, name_s2))
        
        dw_id = cur_dw.lastrowid
        
        hr_records = data['hr']
        fin_record = data['fin']
        
        if not hr_records:
            cur_dw.execute("""
                INSERT INTO fact_employee_profile 
                (dw_employee_id, role, project_name, tech_stack, skills_list, total_income)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (dw_id, None, None, None, fin_record['skills'] if fin_record else None, fin_record['income'] if fin_record else 0))
        else:
            for hr in hr_records:
                cur_dw.execute("""
                    INSERT INTO fact_employee_profile 
                    (dw_employee_id, role, project_name, tech_stack, skills_list, total_income)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    dw_id, 
                    hr['role'], 
                    hr['project'], 
                    hr['stack'], 
                    fin_record['skills'] if fin_record else None, 
                    fin_record['income'] if fin_record else 0
                ))
                
    conn_dw.commit()
    
    # ВЫВОД РЕЗУЛЬТАТОВ

    print("\n--- РЕЗУЛЬТАТЫ В ХД (DW) ---")

    cur_dw.execute("""
        SELECT 
            d.name_signature, 
            d.name_source_1, 
            d.name_source_2, 
            f.role, 
            f.project_name,
            f.skills_list,
            f.total_income
        FROM dim_employee d
        JOIN fact_employee_profile f ON d.dw_id = f.dw_employee_id
    """)
    
    rows = cur_dw.fetchall()
    
    # Заголовок таблицы
    print(f"{'Сигнатура':<20} | {'Имя (БД1)':<18} | {'Имя (БД2)':<18} | {'Роль':<18} | {'Проект':<18} | {'Навыки':<25} | {'Доход':<10}")
    print("-" * 145)
    
    for row in rows:
        sig, n1, n2, role, project, skills, income = row
        # Обработка None для красивого вывода
        p_str = str(project) if project else "N/A"
        s_str = str(skills) if skills else "N/A"
        r_str = str(role) if role else "N/A"
        
        print(f"{sig:<20} | {str(n1):<18} | {str(n2):<18} | {r_str:<18} | {p_str:<18} | {s_str:<25} | {income:<10.2f}")

    conn1.close()
    conn2.close()
    conn_dw.close()

# --- ГЛАВНАЯ ФУНКЦИЯ ---
if __name__ == "__main__":
    try:
        clean_db_files()
        time.sleep(0.5)
        create_source_1()
        create_source_2()
        create_dw()
        etl_process()
        print("\n Программа завершена успешно!")
    except PermissionError as e:
        print(f"\n Ошибка доступа к файлу: {e}")
        print("Закройте все программы, которые могут использовать .db файлы")
    except Exception as e:
        print(f"\n Произошла ошибка: {e}")
        raise