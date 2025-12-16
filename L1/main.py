from flask import Flask, jsonify
from psycopg2 import pool
import psycopg2
import os
import threading

app = Flask(__name__)

# True - БД, False - пам'ять
USE_DATABASE = False
# USE_DATABASE = True

# Для режиму з пам'яттю
counter_lock = threading.Lock()
memory_counter = 0

# Для режиму з БД
db_pool = None

def init_db_pool():
    """Ініціалізація пулу з'єднань до БД"""
    global db_pool
    
    db_host = os.getenv('DB_HOST', 'localhost')
    db_name = os.getenv('DB_NAME', 'webcounter')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')
    db_port = os.getenv('DB_PORT', '5432')
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            database='postgres', 
            user=db_user,
            password=db_password,
            port=db_port
        )
        conn.autocommit = True  
        cursor = conn.cursor()
        
        # Перевіряємо чи існує БД
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f'CREATE DATABASE {db_name}')
            print(f"База даних '{db_name}' створена")
        else:
            print(f"База даних '{db_name}' вже існує")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Помилка при створенні БД: {e}")
        raise
    
    # Тепер створюємо пул з'єднань до нашої БД
    db_pool = pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=20,
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password,
        port=db_port
    )
    
    # Створення таблиці якщо не існує
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS counter (
                id INTEGER PRIMARY KEY,
                value INTEGER NOT NULL
            );
        """)
        cursor.execute("""
            INSERT INTO counter (id, value) VALUES (1, 0)
            ON CONFLICT (id) DO NOTHING;
        """)
        conn.commit()
        cursor.close()
        print("Таблиця 'counter' готова")
    finally:
        db_pool.putconn(conn)

def increment_memory():
    """Інкремент каунтера в пам'яті"""
    global memory_counter
    with counter_lock:
        memory_counter += 1

def increment_database():
    """Інкремент каунтера в БД"""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE counter 
            SET value = value + 1 
            WHERE id = 1;
        """)
        conn.commit()
        cursor.close()
    finally:
        db_pool.putconn(conn)

def get_count_memory():
    """Отримати значення каунтера з пам'яті"""
    with counter_lock:
        return memory_counter

def get_count_database():
    """Отримати значення каунтера з БД"""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM counter WHERE id = 1;")
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else 0
    finally:
        db_pool.putconn(conn)

def reset_counter():
    """Скинути каунтер"""
    global memory_counter
    if USE_DATABASE:
        conn = db_pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute("UPDATE counter SET value = 0 WHERE id = 1;")
            conn.commit()
            cursor.close()
        finally:
            db_pool.putconn(conn)
    else:
        with counter_lock:
            memory_counter = 0

@app.route('/inc')
def increment():
    """Endpoint для інкременту counter"""
    if USE_DATABASE:
        increment_database()
    else:
        increment_memory()
    return jsonify({'status': 'incremented'})

@app.route('/count')
def count():
    """Endpoint для отримання значення counter"""
    if USE_DATABASE:
        value = get_count_database()
    else:
        value = get_count_memory()
    return jsonify({'count': value})

@app.route('/reset')
def reset():
    """Endpoint для скидання counter"""
    reset_counter()
    return jsonify({'status': 'reset', 'count': 0})

@app.route('/mode')
def mode():
    """Endpoint для перевірки режиму роботи"""
    mode_str = 'database' if USE_DATABASE else 'memory'
    return jsonify({'mode': mode_str})

if __name__ == '__main__':
    if USE_DATABASE:
        print("Запуск сервера в режимі БД (PostgreSQL)")
        init_db_pool()
    else:
        print("Запуск сервера в режимі пам'яті")
    
    print("> Запуск сервера на http://0.0.0.0:8080")
    
    try:
        from waitress import serve
        print("> Використовується Waitress (production mode)")
        serve(app, host='0.0.0.0', port=8080, threads=20)
    except ImportError:
        print("> Waitress не знайдено, використовується Flask dev server")
        app.run(host='0.0.0.0', port=8080, threaded=True, debug=False, use_reloader=False)