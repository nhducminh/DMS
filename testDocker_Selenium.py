from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Khởi tạo đối tượng ChromeOptions
chrome_options = Options()

# Thêm các tùy chọn thông qua phương thức add_argument
chrome_options.add_argument("--headless")  # Chạy Chrome ở chế độ không giao diện (headless)
chrome_options.add_argument("--disable-gpu")  # Tắt GPU để tăng tốc độ trên các máy không có GPU hoặc khi chạy trong container


# Khởi tạo WebDriver với các tùy chọn đã thiết lập
driver = webdriver.Remote(
    command_executor='http://localhost:4444/wd/hub',
    options=chrome_options
)
# Bây giờ bạn có thể sử dụng driver để tương tác với trình duyệt
website = driver.get("http://gvn.co/")
print(website)

# Đóng trình duyệt sau khi hoàn thành tests
driver.quit()
