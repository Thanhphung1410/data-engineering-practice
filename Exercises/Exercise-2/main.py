import requests
import pandas as pd
from bs4 import BeautifulSoup

URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
TIMESTAMP = "2024-01-19 10:27"

# Tìm tên file theo timestamp
soup = BeautifulSoup(requests.get(URL).text, "html.parser")
file_tag = next((
    row.find("a") for row in soup.find_all("tr")
    if row.find_all("td")[1].text.strip() == TIMESTAMP
), None)

if not file_tag:
    print("Không tìm thấy file với timestamp yêu cầu.")
    exit(1)

filename = file_tag["href"]
file_url = URL + filename
local_file = "data.csv"

# Tải file
with open(local_file, "wb") as f:
    f.write(requests.get(file_url).content)

# Đọc file và xử lý
df = pd.read_csv(local_file)
if "HourlyDryBulbTemperature" in df.columns:
    max_temp = df["HourlyDryBulbTemperature"].max()
    print(df[df["HourlyDryBulbTemperature"] == max_temp])
else:
    print("Không tìm thấy cột HourlyDryBulbTemperature.")
