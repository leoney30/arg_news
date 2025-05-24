import emailjs
from datetime import datetime, timedelta
import csv
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

def send_news_email(csv_filename):
    """
    发送近两天未发送的新闻到指定邮箱
    
    Args:
        csv_filename (str): CSV文件名
    """
    # EmailJS配置
    EMAILJS_USER_ID = os.getenv('EMAILJS_USER_ID')
    EMAILJS_TEMPLATE_ID = os.getenv('EMAILJS_TEMPLATE_ID')
    EMAILJS_SERVICE_ID = os.getenv('EMAILJS_SERVICE_ID')
    RECIPIENT_EMAIL = os.getenv('RECIPIENT_EMAIL')
    
    if not all([EMAILJS_USER_ID, EMAILJS_TEMPLATE_ID, EMAILJS_SERVICE_ID, RECIPIENT_EMAIL]):
        print("错误：请确保在.env文件中设置了所有必需的EmailJS配置")
        return
    
    # 获取近两天的日期范围
    today = datetime.now()
    two_days_ago = today - timedelta(days=2)
    
    # 读取CSV文件并筛选符合条件的新闻
    news_to_send = []
    rows_to_update = []
    
    try:
        with open(csv_filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            fieldnames = list(reader.fieldnames)
            if '发送状态' not in fieldnames:
                fieldnames.append('发送状态')
            
            for row in reader:
                # 创建新的行字典
                new_row = dict(row)
                if '发送状态' not in new_row:
                    new_row['发送状态'] = ''
                
                # 如果已经发送过，跳过
                if new_row['发送状态'].strip() == '已发送':
                    rows_to_update.append(new_row)
                    continue
                
                # 检查日期
                try:
                    article_date = datetime.strptime(row['新闻日期'], '%Y-%m-%d')
                    if article_date >= two_days_ago:
                        news_to_send.append({
                            'title': row['新闻标题'],
                            'link': row['新闻链接']
                        })
                        new_row['发送状态'] = '已发送'
                except (ValueError, KeyError):
                    pass
                
                rows_to_update.append(new_row)
        
        if not news_to_send:
            print("没有需要发送的新新闻")
            return
        
        # 准备邮件内容
        email_content = "近两天的新闻更新：\n\n"
        for news in news_to_send:
            email_content += f"标题：{news['title']}\n"
            email_content += f"链接：{news['link']}\n\n"
        
        # 发送邮件
        emailjs.send(
            EMAILJS_SERVICE_ID,
            EMAILJS_TEMPLATE_ID,
            {
                'to_email': RECIPIENT_EMAIL,
                'subject': f"新闻更新 - {today.strftime('%Y-%m-%d')}",
                'message': email_content
            },
            EMAILJS_USER_ID
        )
        
        print(f"成功发送 {len(news_to_send)} 条新闻")
        
        # 更新CSV文件中的发送状态
        with open(csv_filename, 'w', encoding='utf-8', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows_to_update)
            
        print("已更新CSV文件中的发送状态")
        
    except FileNotFoundError:
        print(f"错误：找不到文件 {csv_filename}")
    except Exception as e:
        print(f"发送邮件时发生错误：{str(e)}")

if __name__ == "__main__":
    csv_filename = "messi_argentina_news.csv"
    send_news_email(csv_filename) 