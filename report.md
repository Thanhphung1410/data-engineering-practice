Báo cáo các bài tập 
Pipeline chạy lần lượt các Exercise-1, Exercise-2, Exercise-3, Exercise-4, Exercise-5, Exercise-6, Exercise-7  

![image](https://github.com/user-attachments/assets/de68dfb2-3816-4e10-a3ff-79937866a757)
Exercise 1
Sau khi thực hiện lệnh docker build --tag=exercise-1 ., thêm mã vào file main.py và chạy docker-compose up, các file zip đã được tải xuống (ngoại trừ một liên kết bị lỗi: https://divvy tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip). Các file zip được lưu trong thư mục downloads_url 
![image](https://github.com/user-attachments/assets/5145c658-a418-4f9d-8f42-6ff6cd2de2f2) 

![image](https://github.com/user-attachments/assets/71779eac-82ea-4010-93a2-346b3c6c8f7d)

Exercise 2
Sau khi docker build --tag=exercise-2 ., thêm code vào main.py và chạy docker-compose up. Cào dữ liệu từ website và lưu vào file CSV.

![image](https://github.com/user-attachments/assets/f0c0b6bc-d25d-4e6c-9824-99b5f3fd891d) 

![image](https://github.com/user-attachments/assets/7bd37867-9168-435f-8bcd-2402e6b4bae1)  

Exercise 3 
Sau khi chạy lệnh docker build --tag=exercise3 ., bạn thêm mã xử lý vào main.py để tải và giải nén dữ liệu từ Common Crawl. Khi thực thi docker-compose up trong thư mục EXERCISE-3, chương trình sẽ tự động tải file wet.paths.gz từ S3 và tạo ba thư mục: gzip_files (chứa file .gz tải về), path_files (chứa file .txt giải nén, liệt kê đường dẫn WET), và data_files (lưu nội dung .wet đã giải nén từ web).
![image](https://github.com/user-attachments/assets/6f89ae19-518e-40fb-a903-8d637c779a94) 

![image](https://github.com/user-attachments/assets/a8ef5d9b-4d0b-478a-a590-74fbc79141a8)

EXERCISE-4 
Sau khi docker build --tag=exercise4 ., thêm code vào main.py và chạy docker-compose up. Tìm tất cả file JSON trong thư mục data và các thư mục con, sau đó chuyển từng file sang CSV.
![image](https://github.com/user-attachments/assets/f70688ee-a0d3-42f4-b5db-f1723e060a4a) 

![image](https://github.com/user-attachments/assets/62bded2a-dff1-44d8-9719-fbbea108605c) 

![image](https://github.com/user-attachments/assets/80328444-5bf9-4c59-a06e-65ed56b96e0f)

Exercise 5 \
Sau khi docker build --tag=exercise5 ., thêm code vào main.py và chạy docker-compose up. Kết nối PostgreSQL và pgAdmin4, tạo bảng cho 3 file CSV trong data và insert dữ liệu tương ứng.

![image](https://github.com/user-attachments/assets/28c0fe27-184e-46d1-93ef-cfb6e398b647)

![image](https://github.com/user-attachments/assets/66491231-4e28-4e9c-8e58-1abd00dd736c) 

![image](https://github.com/user-attachments/assets/c7a95e9b-fc53-4263-9a4a-ebc136938f3f) 

![image](https://github.com/user-attachments/assets/4c0d95ce-9b28-4f54-9d9b-e4258628b84d) 

![image](https://github.com/user-attachments/assets/82dad750-b050-4193-910d-0f2692d40906) 

Exercise 6 \
Sau khi docker build --tag=exercise6 ., thêm code  vào main.py và chạy docker-compose up.Dữ liệu được lưu vào file csv và viết filecode PySpark. Sinh ra file report , trong file report gồm các file dữ liệu

 ![image](https://github.com/user-attachments/assets/3d5276f3-f857-4fe8-99de-228e4ac998d8) 
 
 ![image](https://github.com/user-attachments/assets/5a82018e-880b-4da2-9242-1ef7cc0e1d56) 

 File dữ liệu trong file report 
 
 ![image](https://github.com/user-attachments/assets/b28faf48-f531-45b4-8c68-fe5ab454cba8) 
 
 File average_trip_duration_per_day: 
 
![image](https://github.com/user-attachments/assets/f7db606e-9cf3-4e74-9cb0-3e76f9b24eeb) 

File avg_trip_duration_by_gender 

![image](https://github.com/user-attachments/assets/deb31830-f7c3-4147-8e19-a13529e1af42) 

File most_popular_start_station_per_month 

![image](https://github.com/user-attachments/assets/9cfad453-7f5c-4738-bad7-9b684138500b) 

FILE  top_3_popular_end_stations_last_14_days 

![image](https://github.com/user-attachments/assets/a4fb9367-0d2a-4718-9287-2776e8a394a1) 

FILE top_10_ages_longest_trips  

![image](https://github.com/user-attachments/assets/66246738-2cb2-4864-8ca8-6b91c59ee6d0) 

File top_10_ages_shortest_trips 

![image](https://github.com/user-attachments/assets/a2ff6b96-7711-4bc8-9c1c-95bf926a7bd5) 

File trip_count_per_day 

![image](https://github.com/user-attachments/assets/e49e70a6-ffb9-435f-ad62-9ce5bfb5407d) 

Exercise 7 

Sau khi docker build --tag=exercise7 ., thêm code vào main.py và chạy docker-compose up.
Sinh ra các file 

![image](https://github.com/user-attachments/assets/79be0ed8-2ce6-4882-a100-973f88f5986c) 

![image](https://github.com/user-attachments/assets/160e3379-6bd0-4814-80c1-5aba66cddeb3) 

![image](https://github.com/user-attachments/assets/7c46f234-5002-421e-b63a-cc01d93fa0a6)

 báo cáo Lab 8
 
câu 2

Sau khi thực hiện lệnh docker-compose build và docker-compose run currency_ml, pipeline đã chạy hoàn chỉnh :
File dữ liệu đã được tạo: data/rates.csv và File mô hình đã được lưu: model/model.pkl
Quá trình cào dữ liệu, tiền xử lý, huấn luyện mô hình và lưu kết quả đã hoàn tất thành công trong môi trường Docker, và mô hình tạo ra có độ chính xác rất cao (1.00).

![image](https://github.com/user-attachments/assets/ece88165-8157-4f0e-88f0-095f2e32da8a)

![image](https://github.com/user-attachments/assets/49052eb5-6dfb-4a2d-ae7c-9575c31d352a)


![image](https://github.com/user-attachments/assets/25b1cd34-31d0-4161-be6b-2e1597aa97de)


![image](https://github.com/user-attachments/assets/efc5b6af-1f40-48c8-8c18-09a59b2a40e3)













