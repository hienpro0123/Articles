# crawl.py
import requests
from bs4 import BeautifulSoup

def crawl_data():
    url = "https://vnexpress.net/cong-nghe/ai"
    
    # Gửi yêu cầu HTTP để lấy nội dung trang
    snap_shot = requests.get(url)
    raw_html = snap_shot.text

    # Parse HTML với BeautifulSoup
    soup = BeautifulSoup(raw_html, 'html.parser')

    # Lấy tất cả các thẻ <p> có class="description" (giới hạn 5 bài)
    result = soup.find_all('p', class_="description", limit=5)

    # Hàm lấy thông tin chi tiết từ mỗi bài viết
    def infor(url):
        snap_shot = requests.get(url)
        raw_html = snap_shot.text
        soup = BeautifulSoup(raw_html, 'html.parser')
        # Lấy thời gian và tác giả
        get_time = soup.find('span', class_='date').text.strip()
        author = soup.find('strong').text.strip()
        return get_time, author

    data = []
    for quote in result:
        a_tag = quote.find('a')
        # Kiểm tra nếu a_tag không tồn tại hoặc không phải là Tag thì bỏ qua
        if not a_tag or not hasattr(a_tag, 'get'):
            continue
        
        title = a_tag.get('title', 'N/A')
        href = a_tag.get('href', 'N/A')
        summary = a_tag.text.strip()
        try:
            get_time, author = infor(href)
        except Exception as e:
            # Nếu gặp lỗi trong việc lấy thông tin chi tiết, bạn có thể bỏ qua bài viết đó
            print(f"Lỗi khi xử lý bài viết {href}: {e}")
            continue
        data.append({
            "Title": title,
            "Link": href,
            "Date": get_time,
            "Author": author,
            "Summary": summary
        })

    return data

data = crawl_data()
for raw in data:
    print(raw)