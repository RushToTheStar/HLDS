import threading 
import time 
from cassandra.cluster import Cluster 
from cassandra.query import SimpleStatement 
from cassandra import ConsistencyLevel 
import sys 

KEYSPACE = 'keyspace_rf3' 
TABLE = 'likes_counter' 
POST_ID = '123e4567-e89b-12d3-a456-426614174000' 
NUM_CLIENTS = 10 
INCREMENTS_PER_CLIENT = 10000 
TOTAL_EXPECTED = NUM_CLIENTS * INCREMENTS_PER_CLIENT 

def get_consistency_level(): 
    if len(sys.argv) < 2 or sys.argv[1].upper() not in ['ONE', 'QUORUM']: 
        print("Будь ласка, вкажіть 'ONE' або 'QUORUM' як аргумент.") 
        sys.exit(1) 
    if sys.argv[1].upper() == 'ONE': 
        return ConsistencyLevel.ONE 
    else: 
        return ConsistencyLevel.QUORUM 
 
def increment_worker(session, prepared_query): 
    for _ in range(INCREMENTS_PER_CLIENT): 
        try: 
            session.execute(prepared_query) 
        except Exception as e: 
            print(f"Помилка: {e}") 
def run_test(): 
    CONSISTENCY = get_consistency_level() 
    print(f"Тестування з Consistency Level: {ConsistencyLevel.value_to_name[CONSISTENCY]}") 
    cluster = Cluster(['127.0.0.1'], port=9042)  
    session = cluster.connect() 
    session.set_keyspace(KEYSPACE) 
    try: 
        session.execute(f"TRUNCATE {TABLE};") 
        session.execute(f"UPDATE {TABLE} SET likes = likes + 0 WHERE post_id = {POST_ID};") 
        print("Лічильник скинуто до 0.") 
    except Exception as e: 
        print(f"Помилка очищення: {e}") 
        cluster.shutdown() 
        return 
    query_str = f"UPDATE {TABLE} SET likes = likes + 1 WHERE post_id = {POST_ID};" 
    statement = SimpleStatement(query_str, consistency_level=CONSISTENCY) 
 
    threads = [] 
    start_time = time.time() 
    print(f"Запуск {NUM_CLIENTS} клієнтів по {INCREMENTS_PER_CLIENT} інкрементів") 
    for _ in range(NUM_CLIENTS): 
        thread = threading.Thread(target=increment_worker, args=(session, statement)) 
        threads.append(thread) 
        thread.start() 
    for thread in threads: 
        thread.join() 
 
    end_time = time.time() 
    print("Усі клієнти завершили роботу.") 
 
    total_time = end_time - start_time 
    read_statement = SimpleStatement(f"SELECT likes FROM {TABLE} WHERE post_id = {POST_ID}", consistency_level=ConsistencyLevel.QUORUM) 
    result_rows = session.execute(read_statement) 
    actual_value = result_rows.one().likes if result_rows else 0 
 
    print("\nРезультат") 
    print(f"Витрачений час: {total_time:.4f} секунд") 
    print(f"Очікуване значення: {TOTAL_EXPECTED}") 
    print(f"Реальне значення:  {actual_value}") 
 
    if actual_value == TOTAL_EXPECTED: 
        print("Результат збігається!") 
    else: 
        print(f"Результат не збігається. Втрачено {TOTAL_EXPECTED - actual_value} інкрементів.") 
    cluster.shutdown() 

    
if __name__ == "__main__": 
    run_test() 