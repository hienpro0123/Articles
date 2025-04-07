import re
from datetime import datetime
import pandas as pd
from crawl import crawl_data
# Hàm chuyển đổi thời gian từ định dạng VN
def parse_vn_date(date_str):
    # Loại bỏ phần "Thứ ..." và "(GMT+7)"
    date_str = re.sub(r'^(Thứ \w+|Chủ nhật), ', '', date_str)
    date_str = re.sub(r'\s*\(GMT\+\d+\)', '', date_str)

    # Tách ngày và giờ
    try:
        dt = datetime.strptime(date_str, '%d/%m/%Y, %H:%M')
        return dt.strftime('%Y-%m-%d %H:%M')
    except ValueError as e:
        return f"Lỗi: {e}"

# Hàm xử lý dữ liệu để lưu vào DataFrame
def transform_data(data):
    # Duyệt qua từng bài viết và chuyển đổi dữ liệu cần thiết
    transformed_data = []
    for item in data:
        get_time = parse_vn_date(item['Date'])
        transformed_data.append({
            "Title": item["Title"],
            "Link": item["Link"],
            "Date": get_time,
            "Author": item["Author"],
            "Summary": item["Summary"]
        })
    return transformed_data

data = crawl_data()
transformed_data = transform_data(data)
print(transformed_data)