import requests
from bs4 import BeautifulSoup
import csv # 我们会再次使用 csv 模块进行追加写入
import re
import pandas as pd # 主要用于加载初始数据和最后可能的清理
import os
from datetime import datetime, timedelta

# --- 常量定义 ---
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
CSV_COLUMNS = ['新闻标题', '新闻链接', '新闻日期', '状态']

def log_message(message, level="INFO"):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][{level}] {message}")

def load_crawled_links_from_csv(csv_filename):
    """从CSV加载已爬取的链接集合"""
    crawled_links = set()
    if os.path.exists(csv_filename):
        try:
            # 使用 pandas 读取，方便处理列名和空文件
            df = pd.read_csv(csv_filename)
            if '新闻链接' in df.columns and not df.empty:
                crawled_links.update(df['新闻链接'].astype(str).dropna().tolist())
            log_message(f"从 '{csv_filename}' 加载了 {len(crawled_links)} 个历史链接用于去重。")
        except pd.errors.EmptyDataError:
            log_message(f"警告: CSV文件 '{csv_filename}' 为空。")
        except Exception as e:
            log_message(f"读取CSV文件 '{csv_filename}' 以加载链接时出错: {e}", "ERROR")
    return crawled_links

def extract_date_from_url(url):
    match = re.search(r'/(\d{4}-\d{2}-\d{2})/', url)
    if match:
        return match.group(1)
    match_saishi = re.search(r'/(saishi|news)/(\d{4})/(\d{2})(\d{2})', url)
    if match_saishi:
        return f"{match_saishi.group(2)}-{match_saishi.group(3)}-{match_saishi.group(4)}"
    return None

def scrape_zhibo8(url, keywords, csv_filename):
    """
    爬取直播吧网站新闻，如果新闻链接未在CSV中出现过，则追加到CSV文件。
    保留CSV中已有的数据和状态。
    """
    # 1. 加载所有已存在于CSV中的链接，用于去重
    already_crawled_links = load_crawled_links_from_csv(csv_filename)
    
    new_items_appended_count = 0

    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=10)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.content, 'html.parser')

        news_containers = soup.find_all('div', class_=['video v_change', 'article_type_video', 'mixed_type_video'])
        if not news_containers:
            news_containers = soup.select('ul.articleList li, div.dataList ul li') # 备用选择器

        log_message(f"找到 {len(news_containers)} 个可能的新闻容器。")

        # 2. 以追加模式打开CSV文件准备写入新数据
        #    如果文件不存在，会自动创建；如果存在，则在末尾追加
        file_exists = os.path.exists(csv_filename)
        with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)

            # 如果文件是新创建的 (或者为空)，写入表头
            if not file_exists or os.path.getsize(csv_filename) == 0:
                csv_writer.writerow(CSV_COLUMNS)
                log_message(f"CSV文件 '{csv_filename}' 为空或不存在，已写入表头。")

            for news_item in news_containers:
                link_tags = news_item.find_all('a', href=True)
                for link_tag in link_tags:
                    title = link_tag.text.strip()
                    link = link_tag['href']

                    if not title: continue # 跳过无标题链接
                    
                    # 链接规范化
                    if link.startswith('//'): link = 'https:' + link
                    elif not link.startswith(('http://', 'https://')):
                        if not link.startswith('/'): link = 'https://news.zhibo8.cc/' + link.lstrip('/')
                        else: link = 'https://news.zhibo8.cc' + link
                    
                    # 关键词过滤
                    if not any(keyword.lower() in title.lower() for keyword in keywords):
                        continue

                    # ***核心去重逻辑***
                    if link in already_crawled_links:
                        # log_message(f"跳过已存在链接: {link[:70]}...") # 调试时可取消注释
                        continue
                    # *******************

                    date = extract_date_from_url(link)
                    if not date: # 简单处理无日期情况
                        # 你可以加入更复杂的日期提取逻辑，或者直接跳过
                        # time_tag = news_item.find(class_=re.compile(r'time|date|label')) ...
                        log_message(f"无法确定日期，跳过新闻: {title[:50]} ({link})")
                        continue
                    
                    # 准备写入新行的数据，状态默认为空
                    new_row = [title, link, date, ''] # 标题, 链接, 日期, 状态(空)
                    csv_writer.writerow(new_row)
                    
                    # 将新写入的链接添加到内存中的集合，防止本次运行内重复添加（尽管概率低）
                    # 并且也方便计数
                    already_crawled_links.add(link) 
                    new_items_appended_count += 1
                    log_message(f"新追加: {title[:50]}...")
        
        if new_items_appended_count > 0:
            log_message(f"成功追加 {new_items_appended_count} 条新新闻到 '{csv_filename}'。")
        else:
            log_message("没有发现新的、不重复的新闻可追加。")

    except requests.exceptions.RequestException as e:
        log_message(f"请求错误: {e}", "ERROR")
    except Exception as e:
        log_message(f"抓取或写入过程中发生错误: {e}", "ERROR")

    # (可选) 最后用pandas清理一下CSV，去除可能因为意外产生的完全重复行 (基于链接)
    # 这一步会重写整个文件，但能保证最终文件的干净
    # 如果你非常信任追加逻辑，或者文件非常大不希望重写，可以跳过这一步
    try:
        if os.path.exists(csv_filename) and os.path.getsize(csv_filename) > 0 : # 确保文件存在且不为空
            df_final = pd.read_csv(csv_filename)
            # 确保列存在，以防万一CSV是手动创建或损坏的
            for col in CSV_COLUMNS:
                if col not in df_final.columns:
                    df_final[col] = ''
            df_final = df_final.reindex(columns=CSV_COLUMNS) # 保证列顺序

            original_rows = len(df_final)
            # 基于链接去重，保留第一个出现的（即原始的，如果它有状态）
            df_final.drop_duplicates(subset=['新闻链接'], keep='first', inplace=True)
            if len(df_final) < original_rows:
                log_message(f"通过pandas清理，移除了 {original_rows - len(df_final)} 条基于链接的重复行。")
            
            df_final.to_csv(csv_filename, index=False, encoding='utf-8')
            log_message(f"最终CSV文件 '{csv_filename}' 已清理并保存。包含 {len(df_final)} 条记录。")
    except Exception as e:
        log_message(f"最终清理CSV文件时发生错误: {e}", "WARN")


if __name__ == '__main__':
    target_url = 'https://news.zhibo8.cc/zuqiu/'
    search_keywords = ['梅西', '阿根廷']
    output_csv_filename = 'messi_argentina_news.csv'

    log_message(f"--- 开始抓取新闻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    scrape_zhibo8(target_url, search_keywords, output_csv_filename)
    log_message(f"--- 新闻抓取完成: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
