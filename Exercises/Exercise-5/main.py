import os
import psycopg2
import csv

# Hàm kiểm tra và tạo thư mục 'data' nếu chưa tồn tại
def ensure_data_directory():
    if not os.path.exists('data'):
        os.makedirs('data')

# Hàm chạy câu lệnh SQL từ file
def run_sql_file(cursor, file_path):
    with open(file_path, 'r') as file:
        sql = file.read()
        cursor.execute(sql)

# Hàm chèn dữ liệu từ file CSV vào database
def insert_csv(cursor, table_name, file_path):
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Bỏ qua header nếu có
        for row in reader:
            query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s'] * len(row))})"
            cursor.execute(query, row)

def main():
    # Đảm bảo thư mục 'data' tồn tại
    ensure_data_directory()
    
    # Kết nối đến PostgreSQL
    conn = psycopg2.connect(
    host='db',
    port='5432',
    dbname='yourdb',
    user='user',
    password='password'
)

    
    cursor = conn.cursor()

    # Chạy các câu lệnh SQL từ file schema.sql
    print("Đang chờ PostgreSQL khởi động...")
    run_sql_file(cursor, 'schema.sql')

    # Nhập dữ liệu từ các file CSV vào các bảng
    insert_csv(cursor, 'students', 'data/students.csv')
    insert_csv(cursor, 'courses', 'data/courses.csv')
    insert_csv(cursor, 'enrollments', 'data/enrollments.csv')

    # Commit và đóng kết nối
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
