import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import os
import sys
import html 

# --- 配置常量 ---
CSV_FILE = 'messi_argentina_news.csv'
SMTP_SERVER = 'smtp.mail.me.com'
SMTP_PORT = 587

def log_message(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def load_environment_variables():
    config = {
        'sender_email': os.getenv('MAIL_USERNAME'),
        'sender_password': os.getenv('MAIL_PASSWORD'),
        'receiver_email': os.getenv('TO_EMAIL'),
    }
    required_vars = ['sender_email', 'sender_password', 'receiver_email']
    missing_vars = [var for var in required_vars if not config[var]]
    if missing_vars:
        log_message(f"错误：以下环境变量未设置或为空：{', '.join(missing_vars)}")
        log_message("请在 GitHub Secrets 中配置这些变量。")
        sys.exit(1)
    return config

def get_recent_news(csv_filepath):
    log_message(f"尝试从 '{csv_filepath}' 读取新闻...")
    if not os.path.exists(csv_filepath):
        log_message(f"警告：CSV 文件 '{csv_filepath}' 不存在。将创建一个空文件或假设没有历史新闻。")
        return pd.DataFrame(columns=['新闻标题', '新闻链接', '新闻日期', '状态'])

    try:
        df = pd.read_csv(csv_filepath)
    except pd.errors.EmptyDataError:
        log_message(f"警告：CSV 文件 '{csv_filepath}' 为空。")
        return pd.DataFrame(columns=['新闻标题', '新闻链接', '新闻日期', '状态'])
    except Exception as e:
        log_message(f"读取 CSV 文件 '{csv_filepath}' 时出错: {e}")
        return pd.DataFrame(columns=['新闻标题', '新闻链接', '新闻日期', '状态'])

    if '新闻日期' not in df.columns:
        log_message(f"错误：CSV 文件 '{csv_filepath}' 中缺少 '新闻日期' 列。")
        return pd.DataFrame(columns=['新闻标题', '新闻链接', '新闻日期', '状态'])
    df['新闻日期'] = pd.to_datetime(df['新闻日期'], errors='coerce')

    if '状态' not in df.columns:
        log_message(f"警告：CSV 文件 '{csv_filepath}' 中缺少 '状态' 列，将自动添加。")
        df['状态'] = ''
    
    today = datetime.now()
    two_days_ago = today - timedelta(days=2)
    
    recent_news = df[
        (df['新闻日期'].notna()) &
        (df['新闻日期'] >= two_days_ago) & 
        (df['状态'].fillna('').str.strip() != '已发送')
    ].copy()
    
    log_message(f"找到 {len(recent_news)} 条近两天内未发送的新闻。")
    return recent_news


def send_email(news_df, config):
    """发送邮件，标题为超链接"""
    if news_df.empty:
        log_message("没有需要发送的新闻。")
        return False
    
    sender_email = config['sender_email']
    sender_password = config['sender_password']
    receiver_email = config['receiver_email']
    
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = f"梅西新闻更新 - {datetime.now().strftime('%Y-%m-%d')}"
    
    html_body = """
    <html>
      <head>
        <style>
          body { font-family: sans-serif; line-height: 1.6; }
          .news-item { margin-bottom: 15px; padding-bottom: 10px; border-bottom: 1px solid #eee; }
          .news-item p { margin: 5px 0; }
          .news-item a { color: #007bff; text-decoration: none; font-weight: bold; }
          .news-item a:hover { text-decoration: underline; }
          .date { font-size: 0.9em; color: #555; }
        </style>
      </head>
      <body>
        <p>以下是近两天的梅西相关新闻：</p>
    """ # (移除了第一个<hr>，因为每个项目后都有)
    
    for _, row in news_df.iterrows():
        title = row['新闻标题']
        link = row['新闻链接']
        date_str = row['新闻日期'].strftime('%Y-%m-%d') if pd.notnull(row['新闻日期']) else "日期未知"
        
        escaped_title = html.escape(title) # 现在 html 模块在顶部已导入

        html_body += f"""
        <div class="news-item">
          <p><a href="{link}" target="_blank">{escaped_title}</a></p>
          <p class="date">日期：{date_str}</p>
        </div>
        """ # (移除了项目内的<hr>，因为class="news-item"已有border-bottom)

    html_body += """
      </body>
    </html>
    """
    
    msg.attach(MIMEText(html_body, 'html', 'utf-8'))
    
    try:
        log_message(f"尝试连接 SMTP 服务器: {SMTP_SERVER}:{SMTP_PORT}")
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30)
        server.ehlo()
        server.starttls()
        server.ehlo()
        log_message("尝试登录邮箱...")
        server.login(sender_email, sender_password)
        log_message("登录成功，正在发送邮件...")
        server.send_message(msg)
        server.quit()
        log_message("邮件发送成功！")
        return True
    except smtplib.SMTPAuthenticationError as e:
        log_message(f"SMTP 认证失败：{e}. 请检查邮箱用户名和密码。")
        return False
    except smtplib.SMTPConnectError as e:
        log_message(f"无法连接到 SMTP 服务器 ({SMTP_SERVER}:{SMTP_PORT})：{e}")
        return False
    except smtplib.SMTPServerDisconnected as e:
        log_message(f"SMTP 服务器意外断开连接：{e}")
        return False
    except Exception as e:
        log_message(f"发送邮件时发生未知错误：{e}")
        return False

def update_news_status_in_csv(news_df_sent, csv_filepath):
    if news_df_sent.empty:
        log_message("没有新闻状态需要更新。")
        return

    if not os.path.exists(csv_filepath):
        log_message(f"错误：CSV 文件 '{csv_filepath}' 不存在，无法更新状态。")
        return

    try:
        df_original = pd.read_csv(csv_filepath)
    except Exception as e:
        log_message(f"读取原始 CSV 文件 '{csv_filepath}' 以更新状态时出错: {e}")
        return

    if '状态' not in df_original.columns:
        df_original['状态'] = ''

    sent_links = news_df_sent['新闻链接'].astype(str).tolist()
    mask = df_original['新闻链接'].astype(str).isin(sent_links)
    df_original.loc[mask, '状态'] = '已发送'
    
    try:
        df_original.to_csv(csv_filepath, index=False, encoding='utf-8')
        log_message(f"新闻状态已更新到 '{csv_filepath}'。")
    except Exception as e:
        log_message(f"保存更新后的 CSV 文件 '{csv_filepath}' 时出错: {e}")


def main():
    log_message("--- 开始执行邮件发送脚本 ---")
    email_config = load_environment_variables()
    csv_file_path = os.path.join(os.getcwd(), CSV_FILE)
    log_message(f"使用的CSV文件路径: {csv_file_path}")
    recent_news_to_send = get_recent_news(csv_file_path)
    if recent_news_to_send.empty:
        log_message("没有符合条件的新闻需要发送。脚本执行完毕。")
        sys.exit(0)
    email_sent_successfully = send_email(recent_news_to_send, email_config)
    if email_sent_successfully:
        update_news_status_in_csv(recent_news_to_send, csv_file_path)
        log_message("--- 邮件发送脚本执行成功 ---")
        sys.exit(0)
    else:
        log_message("邮件发送失败，新闻状态未更新。")
        log_message("--- 邮件发送脚本执行遇到问题 ---")
        sys.exit(1)

if __name__ == "__main__":
    main()
