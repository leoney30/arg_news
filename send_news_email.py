import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import os
import sys # 用于退出码

# --- 配置常量 ---
CSV_FILE = 'messi_argentina_news.csv' # CSV文件名，可以根据实际情况调整
SMTP_SERVER = 'smtp.mail.me.com'
SMTP_PORT = 587

def log_message(message):
    """打印带时间戳的日志信息"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def load_environment_variables():
    """从环境变量加载配置，适配 GitHub Actions Secrets"""
    config = {
        'sender_email': os.getenv('MAIL_USERNAME'), # 对应 GitHub Secret: MAIL_USERNAME
        'sender_password': os.getenv('MAIL_PASSWORD'), # 对应 GitHub Secret: MAIL_PASSWORD
        'receiver_email': os.getenv('TO_EMAIL'),     # 对应 GitHub Secret: TO_EMAIL
        # 你可以根据需要添加其他配置，比如 SMTP_SERVER, SMTP_PORT 如果想从环境变量配置
    }
    
    # 检查必要的环境变量是否都已设置
    required_vars = ['sender_email', 'sender_password', 'receiver_email']
    missing_vars = [var for var in required_vars if not config[var]]
    
    if missing_vars:
        log_message(f"错误：以下环境变量未设置或为空：{', '.join(missing_vars)}")
        log_message("请在 GitHub Secrets 中配置这些变量。")
        sys.exit(1) # 脚本异常退出
        
    return config

def get_recent_news(csv_filepath):
    """获取近两天且未发送的新闻"""
    log_message(f"尝试从 '{csv_filepath}' 读取新闻...")
    if not os.path.exists(csv_filepath):
        log_message(f"警告：CSV 文件 '{csv_filepath}' 不存在。将创建一个空文件或假设没有历史新闻。")
        # 如果文件不存在，可以创建一个带表头的空文件，避免后续读取错误
        # 或者根据你的抓取脚本逻辑，它可能总会创建这个文件
        # 这里我们假设如果文件不存在，则没有新闻可发送
        return pd.DataFrame(columns=['新闻标题', '新闻链接', '新闻日期', '状态']) # 返回空DataFrame

    try:
        df = pd.read_csv(csv_filepath)
    except pd.errors.EmptyDataError:
        log_message(f"警告：CSV 文件 '{csv_filepath}' 为空。")
        return pd.DataFrame(columns=['新闻标题', '新闻链接', '新闻日期', '状态'])
    except Exception as e:
        log_message(f"读取 CSV 文件 '{csv_filepath}' 时出错: {e}")
        return pd.DataFrame(columns=['新闻标题', '新闻链接', '新闻日期', '状态'])


    # 确保 '新闻日期' 列存在且可以转换为日期时间
    if '新闻日期' not in df.columns:
        log_message(f"错误：CSV 文件 '{csv_filepath}' 中缺少 '新闻日期' 列。")
        return pd.DataFrame(columns=['新闻标题', '新闻链接', '新闻日期', '状态'])
    df['新闻日期'] = pd.to_datetime(df['新闻日期'], errors='coerce') # errors='coerce' 会将无效日期转为NaT

    # 如果 '状态' 列不存在，则添加它，并填充为空字符串
    if '状态' not in df.columns:
        log_message(f"警告：CSV 文件 '{csv_filepath}' 中缺少 '状态' 列，将自动添加。")
        df['状态'] = ''
    
    # 获取当前日期
    today = datetime.now()
    # 考虑到 Actions 可能在UTC时间运行，而新闻日期可能是本地时间，
    # 这里假设新闻日期已经是统一时区，或者在抓取时已处理
    two_days_ago = today - timedelta(days=2)
    
    # 筛选近两天且状态为空或非'已发送'的新闻
    # 使用 .fillna('') 处理可能存在的 NaN 值，确保比较的一致性
    recent_news = df[
        (df['新闻日期'].notna()) & # 确保日期有效
        (df['新闻日期'] >= two_days_ago) & 
        (df['状态'].fillna('').str.strip() != '已发送') # 更加鲁棒的状态检查
    ].copy() # 使用 .copy() 避免 SettingWithCopyWarning
    
    log_message(f"找到 {len(recent_news)} 条近两天内未发送的新闻。")
    return recent_news

def send_email(news_df, config):
    """发送邮件"""
    if news_df.empty:
        log_message("没有需要发送的新闻。")
        return False # 返回False表示未发送邮件
    
    sender_email = config['sender_email']
    sender_password = config['sender_password']
    receiver_email = config['receiver_email']
    
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = f"梅西新闻更新 - {datetime.now().strftime('%Y-%m-%d')}"
    
    body_parts = ["以下是近两天的梅西相关新闻：\n"]
    for _, row in news_df.iterrows():
        body_parts.append(f"标题：{row['新闻标题']}")
        body_parts.append(f"链接：{row['新闻链接']}")
        body_parts.append(f"日期：{row['新闻日期'].strftime('%Y-%m-%d')}") # 确保row['新闻日期']是datetime对象
        body_parts.append("-" * 50 + "\n")
    
    body = "\n".join(body_parts)
    msg.attach(MIMEText(body, 'plain', 'utf-8')) # 指定编码为utf-8
    
    try:
        log_message(f"尝试连接 SMTP 服务器: {SMTP_SERVER}:{SMTP_PORT}")
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) # 增加超时设置
        server.ehlo()
        server.starttls()
        server.ehlo()
        log_message("尝试登录邮箱...")
        server.login(sender_email, sender_password)
        log_message("登录成功，正在发送邮件...")
        server.send_message(msg)
        server.quit()
        log_message("邮件发送成功！")
        return True # 返回True表示邮件发送成功
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
    """更新已发送新闻的状态到CSV文件"""
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

    # 确保 '状态' 列存在
    if '状态' not in df_original.columns:
        df_original['状态'] = ''

    # 更新状态
    # 使用新闻链接作为唯一标识符可能更可靠，标题可能重复或微小变动
    # 确保 news_df_sent 中的 '新闻链接' 列和 df_original 中的 '新闻链接' 列类型一致
    sent_links = news_df_sent['新闻链接'].astype(str).tolist()
    
    # 更新 df_original 中匹配链接的行
    # 注意：这里假设 '新闻链接' 是唯一的，如果不是，可能需要更复杂的匹配逻辑
    mask = df_original['新闻链接'].astype(str).isin(sent_links)
    df_original.loc[mask, '状态'] = '已发送'
    
    try:
        df_original.to_csv(csv_filepath, index=False, encoding='utf-8')
        log_message(f"新闻状态已更新到 '{csv_filepath}'。")
    except Exception as e:
        log_message(f"保存更新后的 CSV 文件 '{csv_filepath}' 时出错: {e}")


def main():
    log_message("--- 开始执行邮件发送脚本 ---")
    
    # 1. 加载配置
    email_config = load_environment_variables() # 如果失败，脚本会在此处退出
    
    # 2. 获取需要发送的新闻
    # 假设 CSV 文件与脚本在同一目录或由抓取脚本生成在工作区的固定位置
    csv_file_path = os.path.join(os.getcwd(), CSV_FILE) # 确保路径正确
    log_message(f"使用的CSV文件路径: {csv_file_path}")

    recent_news_to_send = get_recent_news(csv_file_path)
    
    if recent_news_to_send.empty:
        log_message("没有符合条件的新闻需要发送。脚本执行完毕。")
        sys.exit(0) # 正常退出，没有错误
    
    # 3. 发送邮件
    email_sent_successfully = send_email(recent_news_to_send, email_config)
    
    # 4. 如果邮件发送成功，则更新新闻状态
    if email_sent_successfully:
        update_news_status_in_csv(recent_news_to_send, csv_file_path)
        log_message("--- 邮件发送脚本执行成功 ---")
        sys.exit(0) # 正常退出
    else:
        log_message("邮件发送失败，新闻状态未更新。")
        log_message("--- 邮件发送脚本执行遇到问题 ---")
        sys.exit(1) # 异常退出

if __name__ == "__main__":
    # 确保pandas等库已安装
    # pip install pandas python-dotenv (本地测试用)
    main()
