import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path

# URL của trang chứa dữ liệu
URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
# Timestamp cần tìm
TARGET_TIMESTAMP = "2024-01-19 10:27"

def main():
    # Gửi yêu cầu tải nội dung HTML của trang web
    response = requests.get(URL)
    if response.status_code != 200:
        print("Không thể truy cập trang web.")
        return

    # Dùng BeautifulSoup để phân tích cú pháp HTML
    soup = BeautifulSoup(response.text, "html.parser")

    # Tìm tất cả các dòng (tr) trong bảng HTML
    rows = soup.find_all("tr")

    # Duyệt từng dòng để tìm đúng dòng có thời gian Last Modified mong muốn
    target_file = None
    for row in rows:
        columns = row.find_all("td")
        if len(columns) >= 2:
            last_modified = columns[1].text.strip()
            if last_modified == TARGET_TIMESTAMP:
                target_file = columns[0].find("a")["href"]
                break

    if not target_file:
        print("Không tìm thấy file với thời gian Last Modified mong muốn.")
        return

    print(f"Tìm thấy file: {target_file}")

    # Xây dựng URL đầy đủ và tải file
    file_url = URL + target_file
    file_path = Path("weather_data.csv")

    print(f"📥 Đang tải file từ: {file_url}")
    file_response = requests.get(file_url)
    if file_response.status_code != 200:
        print("Không thể tải file.")
        return

    # Lưu file
    with open(file_path, "wb") as f:
        f.write(file_response.content)
    print("Đã lưu file thành công.")

    # Đọc file bằng pandas
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Lỗi khi đọc file CSV: {e}")
        return

    # Kiểm tra xem cột 'HourlyDryBulbTemperature' có tồn tại không
    if "HourlyDryBulbTemperature" not in df.columns:
        print("Không tìm thấy cột 'HourlyDryBulbTemperature' trong dữ liệu.")
        return

    # Tìm nhiệt độ cao nhất theo cột HourlyDryBulbTemperature
    max_temp = df["HourlyDryBulbTemperature"].max()
    hottest_rows = df[df["HourlyDryBulbTemperature"] == max_temp]

    # In các dòng có nhiệt độ cao nhất
    print("Các dòng có nhiệt độ cao nhất:")
    print(hottest_rows)

if __name__ == "__main__":
    main()
