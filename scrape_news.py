import requests
from bs4 import BeautifulSoup
import csv # 你可能不再直接使用 csv 模块了，因为 pandas 处理了 CSV
import re
import pandas as pd
import os
from datetime import datetime, timedelta

# --- 常量定义 ---
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
CSV_COLUMNS = ['新闻标题', '新闻链接', '新闻日期', '状态'] # 统一列名，加入'状态'

def log_message(message, level="INFO"):
    """简单的日志函数"""
    print(f"[{level}] {message}")

def load_existing_data(csv_filename):
    """加载已有的CSV数据和已爬取的链接"""
    existing_df = pd.DataFrame(columns=CSV_COLUMNS) # 默认空DataFrame
    crawled_links_set = set()

    if os.path.exists(csv_filename):
        try:
            existing_df = pd.read_csv(csv_filename)
            # 确保所有必要的列都存在，特别是 '状态'
            for col in CSV_COLUMNS:
                if col not in existing_df.columns:
                    existing_df[col] = '' # 如果列不存在，则添加并填充空值
            
            # 加载已有的链接用于去重
            if '新闻链接' in existing_df.columns:
                crawled_links_set.update(existing_df['新闻链接'].astype(str).tolist())
            log_message(f"从 '{csv_filename}' 加载了 {len(existing_df)} 条已有记录，{len(crawled_links_set)} 个唯一链接。")
        except pd.errors.EmptyDataError:
            log_message(f"警告: CSV文件 '{csv_filename}' 为空，将创建新文件。")
            existing_df = pd.DataFrame(columns=CSV_COLUMNS) # 确保是带列的空DataFrame
        except Exception as e:
            log_message(f"读取CSV文件 '{csv_filename}' 时发生错误: {e}。将尝试创建新文件。", "ERROR")
            existing_df = pd.DataFrame(columns=CSV_COLUMNS) # 出错也返回带列的空DataFrame
    else:
        log_message(f"CSV文件 '{csv_filename}' 不存在，将创建新文件。")
    
    # 确保DataFrame的列顺序与CSV_COLUMNS一致
    existing_df = existing_df.reindex(columns=CSV_COLUMNS)
    return existing_df, crawled_links_set

def extract_date_from_url(url):
    """从URL中提取日期"""
    match = re.search(r'/(\d{4}-\d{2}-\d{2})/', url)
    if match:
        return match.group(1)
    # 可以添加更多日期提取规则
    match_saishi = re.search(r'/(saishi|news)/(\d{4})/(\d{2})(\d{2})', url)
    if match_saishi:
        return f"{match_saishi.group(2)}-{match_saishi.group(3)}-{match_saishi.group(4)}"
    return None

def scrape_zhibo8(url, keywords, csv_filename):
    """
    爬取直播吧网站包含指定关键词的新闻，并将新结果追加到CSV文件中，保留原有状态。
    """
    existing_df, crawled_links = load_existing_data(csv_filename)
    new_news_list = [] # 用于存储新抓取的新闻

    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=10)
        response.raise_for_status()
        response.encoding = response.apparent_encoding # 尝试自动检测编码
        soup = BeautifulSoup(response.content, 'html.parser')

        # 尝试多种可能的class name
        news_containers = soup.find_all('div', class_=['video v_change', 'article_type_video', 'mixed_type_video'])
        if not news_containers: # 如果特定class找不到，尝试更通用的列表项
            news_containers = soup.select('ul.articleList li, div.dataList ul li') # 示例，需根据实际调整

        log_message(f"找到 {len(news_containers)} 个可能的新闻容器。")
        new_items_found_count = 0

        for news_item in news_containers:
            link_tags = news_item.find_all('a', href=True) # 查找所有带href的a标签
            for link_tag in link_tags:
                title = link_tag.text.strip()
                link = link_tag['href']

                if not title: # 跳过没有标题文本的链接
                    continue
                
                # 规范化链接
                if link.startswith('//'):
                    link = 'https:' + link
                elif not link.startswith(('http://', 'https://')):
                    # 尝试拼接基础域名，如果它看起来是相对路径
                    # 对于直播吧，大多数应该是 // 开头或完整路径
                    if not link.startswith('/'):
                        link = 'https://news.zhibo8.cc/' + link.lstrip('/')
                    else:
                        link = 'https://news.zhibo8.cc' + link
                
                # 关键词过滤 (不区分大小写)
                if not any(keyword.lower() in title.lower() for keyword in keywords):
                    continue

                # 去重判断 (与已加载和本次新抓取的比较)
                if link in crawled_links or any(item['新闻链接'] == link for item in new_news_list):
                    # log_message(f"跳过重复或已处理链接: {link_tag.get_text(strip=True)[:30]}...") # 避免打印整个长标题
                    continue

                date = extract_date_from_url(link)
                # 对于没有从URL提取到日期的新闻，可以尝试从父元素或兄弟元素获取
                if not date:
                    time_tag = news_item.find(class_=re.compile(r'time|date|label'))
                    if time_tag:
                        # 这里需要更复杂的日期解析逻辑，以下仅为示例
                        # 例如，如果 time_tag.text 是 "10-25 15:30"
                        # date_match = re.search(r'(\d{2}-\d{2})', time_tag.text)
                        # if date_match:
                        #     current_year = str(pd.Timestamp.now().year)
                        #     date = f"{current_year}-{date_match.group(1)}"
                        log_message(f"从URL无法提取日期: {link}，尝试从附近元素提取 (此部分逻辑可能需完善)。")
                        # 如果仍然无法获取日期，可以选择跳过或使用一个默认/标记值
                        # continue # 或者 date = "日期未知"

                # 如果日期还是没有，可以决定是否跳过
                if not date:
                    log_message(f"无法确定日期，跳过新闻: {title} ({link})")
                    continue

                new_news_data = {
                    '新闻标题': title,
                    '新闻链接': link,
                    '新闻日期': date,
                    '状态': ''  # 新抓取的新闻状态默认为空
                }
                new_news_list.append(new_news_data)
                crawled_links.add(link) # 添加到已爬取集合，防止在同一次运行中重复添加
                log_message(f"发现新新闻: {title[:50]}...")
                new_items_found_count += 1
        
        if new_items_found_count == 0:
            log_message("本次运行未发现符合条件的新新闻。")

    except requests.exceptions.RequestException as e:
        log_message(f"请求错误: {e}", "ERROR")
    except Exception as e:
        log_message(f"抓取过程中发生错误: {e}", "ERROR")

    # 合并旧数据和新数据
    if new_news_list:
        new_df = pd.DataFrame(new_news_list, columns=CSV_COLUMNS)
        # 确保新旧DataFrame的列顺序和类型尽可能一致，以避免concat问题
        # existing_df 和 new_df 的列应该都是 CSV_COLUMNS
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        # 去除可能因为各种原因（例如链接细微差别但内容相同）导致的完全重复行
        combined_df.drop_duplicates(subset=['新闻链接'], keep='last', inplace=True)
        # 也可以基于更多列去重: combined_df.drop_duplicates(subset=['新闻标题', '新闻链接'], keep='last', inplace=True)
    else:
        combined_df = existing_df # 如果没有新数据，就是旧数据

    # 写回CSV文件
    try:
        # 确保列的顺序是我们期望的
        combined_df = combined_df.reindex(columns=CSV_COLUMNS)
        combined_df.to_csv(csv_filename, index=False, encoding='utf-8')
        log_message(f"数据已保存到 '{csv_filename}'。总计 {len(combined_df)} 条记录。")
    except Exception as e:
        log_message(f"保存CSV文件 '{csv_filename}' 时发生错误: {e}", "ERROR")


if __name__ == '__main__':
    target_url = 'https://news.zhibo8.cc/zuqiu/'  # 确保使用 https
    search_keywords = ['梅西', '阿根廷'] # 示例关键词
    output_csv_filename = 'messi_argentina_news.csv'

    log_message(f"--- 开始抓取新闻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    scrape_zhibo8(target_url, search_keywords, output_csv_filename)
    log_message(f"--- 新闻抓取完成: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
