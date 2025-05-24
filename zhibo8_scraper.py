import requests
from bs4 import BeautifulSoup
import csv
import re

# 已爬取的新闻链接集合，用于去重
crawled_links = set()

def is_duplicate(link):
    """
    检查链接是否已经爬取过。
    """
    return link in crawled_links

def extract_date_from_url(url):
    """
    从URL中提取日期。
    """
    match = re.search(r'/(\d{4}-\d{2}-\d{2})/', url)
    if match:
        return match.group(1)
    else:
        return None  # 或者返回一个默认日期，如果URL中没有日期信息

def scrape_zhibo8(url, keywords, csv_filename):
    """
    爬取直播吧网站包含指定关键词的新闻，并将结果保存到CSV文件中。
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        news_list = soup.find_all('div', class_='video v_change')

        with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)

            if csvfile.tell() == 0:
                csv_writer.writerow(['新闻标题', '新闻链接', '新闻日期'])

            for news in news_list:
                links = news.find_all('a', target='_blank')
                for link_tag in links:
                    title = link_tag.text.strip()
                    link = link_tag['href']
                    
                    # 添加https前缀
                    if link.startswith('//'):
                        link = 'https:' + link
                    elif not link.startswith(('http://', 'https://')):
                        link = 'https://' + link

                    # 关键词过滤
                    if not any(keyword in title for keyword in keywords):
                        continue

                    # 去重判断
                    if is_duplicate(link):
                        print(f"跳过重复链接: {link}")
                        continue

                    date = extract_date_from_url(link)
                    if not date:
                        print(f"无法从链接提取日期，跳过: {link}")
                        continue

                    csv_writer.writerow([title, link, date])
                    crawled_links.add(link)
                    print(f"已保存: {title} - {link} - {date}")

    except requests.exceptions.RequestException as e:
        print(f"请求错误: {e}")
    except Exception as e:
        print(f"发生错误: {e}")

if __name__ == '__main__':
    target_url = 'http://news.zhibo8.com/zuqiu/'  #足球新闻页面
    search_keywords = ['梅西', '阿根廷']
    output_csv_filename = 'messi_argentina_news.csv'

    scrape_zhibo8(target_url, search_keywords, output_csv_filename)
    print("爬取完成！")