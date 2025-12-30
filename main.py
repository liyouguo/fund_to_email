import time
import akshare as ak
import pandas as pd
import numpy as np
import requests
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

# 配置参数
BB_WINDOW = 20

# 基金代码列表（100只）
FUND_CODES = [
    '017993', '017994', '001864', '023566', '023567',
    '018124', '018125', '002683', '005164', '005165',
    '007713', '017968', '000550', '000025', '014700',
    '003835', '015401', '015400', '018710', '020419',
    '020420', '007518', '007519', '024647', '024648',
    '001270', '001271', '021981', '021982', '005296',
    '005297', '001888', '166011', '004233', '000649',
    '017461', '016635', '018681', '000976', '019412',
    '002053', '000538', '017063', '017064', '015394',
    '519767', '009092', '014243', '016477', '016478',
    '016075', '016076', '018240', '018241', '016541',
    '016542', '020972', '020973', '025317', '025318',
    '005542', '020893', '005541', '020981', '020982',
    '020894', '012905', '025698', '025700', '025699',
    '012906', '020005', '015589', '010076', '010077',
    '519690', '010746', '019792', '001518', '015390',
    '162207', '017726', '017727', '002839', '002838',
    '014939', '014938'
]

# 邮箱配置（直接在代码中配置）
EMAIL_SENDER = '724429664@qq.com'  # 发件人邮箱
EMAIL_RECEIVER = '724429664@qq.com'  # 收件人邮箱
EMAIL_PASSWORD = 'rohrdfaljywhbfgf'  # 发件人邮箱密码/授权码
EMAIL_SMTP_SERVER = 'smtp.qq.com'  # SMTP服务器地址
EMAIL_SMTP_PORT = 465  # SMTP服务器端口

# 获取历史净值数据
def get_historical_nav(fund_code):
    try:
        # 使用akshare获取单只基金的历史净值数据
        df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
        # 筛选最新的BB_WINDOW条数据
        df = df.tail(BB_WINDOW).sort_values('净值日期', ascending=True)
        # 重置索引
        df.reset_index(drop=True, inplace=True)
        return df
    except Exception as e:
        print(f"获取基金{fund_code}历史净值失败: {e}")
        return None

# 获取当日实时估值
def get_realtime_estimate(fund_code):
    try:
        # 构造请求URL
        timestamp = int(time.time() * 1000)
        url = f"http://fundgz.1234567.com.cn/js/{fund_code}.js?rt={timestamp}"
        # 发送请求
        response = requests.get(url, timeout=10)
        response.encoding = 'utf-8'
        # 解析JSONP数据
        text = response.text.strip()
        # 处理末尾的分号
        if text.endswith(');'):
            text = text[:-1]
        if text.startswith('jsonpgz(') and text.endswith(')'):
            json_str = text[8:-1]  # 移除jsonpgz()包装
            # 使用更安全的方法解析JSON
            import json
            data = json.loads(json_str)
            return data
        else:
            print(f"基金{fund_code}实时估值数据格式异常: {text}")
            return None
    except Exception as e:
        print(f"获取基金{fund_code}实时估值失败: {e}")
        return None

# 计算布林带指标
def calculate_bollinger_bands(data, column='单位净值'):
    # 计算移动平均线
    data['MA'] = data[column].rolling(window=BB_WINDOW).mean()
    # 计算标准差
    data['STD'] = data[column].rolling(window=BB_WINDOW).std()
    # 计算上轨和下轨
    data['Upper Band'] = data['MA'] + 2 * data['STD']
    data['Lower Band'] = data['MA'] - 2 * data['STD']
    return data

# 生成交易信号
def generate_signals(data, column='单位净值'):
    signals = []
    for i in range(1, len(data)):
        today = data.iloc[i]
        yesterday = data.iloc[i-1]
        
        # 检查信号条件
        # 当日值下穿布林带下轨（前一日值≥下轨）
        if today[column] < today['Lower Band'] and yesterday[column] >= yesterday['Lower Band']:
            signals.append({'信号日期': today['净值日期'], 
                           '信号内容': '留意机会', '信号类型': '机会信号'})
        # 当日值上穿布林带下轨（前一日值≤下轨）
        elif today[column] > today['Lower Band'] and yesterday[column] <= yesterday['Lower Band']:
            signals.append({'信号日期': today['净值日期'], 
                           '信号内容': '买入信号', '信号类型': '买入信号'})
        # 当日值上穿布林带上轨（前一日值≤上轨）
        elif today[column] > today['Upper Band'] and yesterday[column] <= yesterday['Upper Band']:
            signals.append({'信号日期': today['净值日期'], 
                           '信号内容': '注意风险', '信号类型': '风险信号'})
        # 当日值下穿布林带上轨（前一日值≥上轨）
        elif today[column] < today['Upper Band'] and yesterday[column] >= yesterday['Upper Band']:
            signals.append({'信号日期': today['净值日期'], 
                           '信号内容': '卖出信号', '信号类型': '卖出信号'})
    return signals

# 处理单只基金数据
def process_fund(fund_code):
    print(f"开始处理基金: {fund_code}")
    # 获取历史净值数据
    historical_data = get_historical_nav(fund_code)
    if historical_data is None or len(historical_data) < BB_WINDOW:
        print(f"基金{fund_code}历史数据不足，跳过")
        return None
    
    # 获取今日日期
    today = datetime.now().strftime('%Y-%m-%d')
    latest_date = historical_data['净值日期'].iloc[-1].strftime('%Y-%m-%d')
    
    # 初始化变量
    is_estimate = False
    use_today_data = False
    
    # 检查是否需要更新今日估值
    if latest_date == today:
        # 使用原始BB_WINDOW条数据计算，生成历史数据信号
        print(f"基金{fund_code}今日已有净值数据，生成历史数据信号")
        use_today_data = True
    else:
        # 调用实时估值接口
        realtime_data = get_realtime_estimate(fund_code)
        if realtime_data and 'gsz' in realtime_data:
            # 接口成功且gsz有效
            print(f"基金{fund_code}获取实时估值成功，生成预估信号")
            # 创建今日估值数据
            today_data = pd.DataFrame({
                '净值日期': [today],
                '单位净值': [float(realtime_data['gsz'])]
            })
            # 重构数据窗口：剔除最早1条，加入今日估值
            historical_data = pd.concat([historical_data.iloc[1:], today_data], ignore_index=True)
            is_estimate = True
            use_today_data = True
        else:
            # 接口失败或gsz无效，直接用原始数据计算，但今日不新增信号
            print(f"基金{fund_code}获取实时估值失败，生成历史数据信号但今日不新增")
            use_today_data = False
    
    # 计算布林带指标
    historical_data = calculate_bollinger_bands(historical_data)
    
    # 生成信号
    signals = generate_signals(historical_data)
    
    if signals:
        # 构造结果数据
        result = []
        for signal in signals:
            latest_row = historical_data.iloc[-1]
            # 确定信号日期
            signal_date = signal['信号日期']
            signal_date_str = signal_date.strftime('%Y-%m-%d') if isinstance(signal_date, datetime) else signal_date
            
            # 只有当信号日期是今日或使用今日数据时才返回结果
            if use_today_data or signal_date_str == today:
                # 确定信号类型描述
                signal_type_desc = '预估信号' if is_estimate else '历史数据信号'
                
                result.append({
                    '基金代码': fund_code,
                    '信号日期': signal_date,
                    '当前净值/估值': latest_row['单位净值'],
                    '布林中轨': latest_row['MA'],
                    '布林上轨': latest_row['Upper Band'],
                    '布林下轨': latest_row['Lower Band'],
                    '信号内容': signal['信号内容'],
                    '信号类型': signal_type_desc
                })
        
        # 只有当有符合条件的信号时才返回
        if result:
            return result
    return None

# 生成Excel文件
def generate_excel(results):
    # 生成文件名
    today = datetime.now().strftime('%Y%m%d')
    filename = f"基金布林带信号_{today}.xlsx"
    
    if not results:
        # 没有信号时，生成包含提示信息的Excel
        df = pd.DataFrame([{
            '基金代码': '无',
            '信号日期': today,
            '当前净值/估值': '无',
            '布林中轨': '无',
            '布林上轨': '无',
            '布林下轨': '无',
            '信号内容': '今日无交易信号',
            '信号类型': '无信号'
        }])
        print("没有生成任何交易信号，生成提示信息Excel")
    else:
        # 有信号时，合并所有结果
        df = pd.DataFrame(results)
        print(f"生成了{len(results)}个交易信号")
    
    # 写入Excel
    df.to_excel(filename, index=False)
    print(f"Excel文件生成成功: {filename}")
    return filename

# 发送邮件
def send_email(excel_file):
    if not excel_file:
        print("没有Excel文件，跳过邮件发送")
        return False
    
    try:
        # 创建邮件对象
        msg = MIMEMultipart()
        msg['From'] = EMAIL_SENDER
        msg['To'] = EMAIL_RECEIVER
        msg['Subject'] = f"基金布林带量化信号_{datetime.now().strftime('%Y-%m-%d')}"
        
        # 添加邮件正文
        body = "今日基金布林带量化信号已生成，请查收附件。"
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        # 添加附件
        with open(excel_file, 'rb') as f:
            attachment = MIMEApplication(f.read(), _subtype='xlsx')
            attachment.add_header('Content-Disposition', 'attachment', filename=excel_file)
            msg.attach(attachment)
        
        # 发送邮件（使用SMTP_SSL连接，添加调试信息）
        print(f"尝试发送邮件，SMTP服务器: {EMAIL_SMTP_SERVER}, 端口: {EMAIL_SMTP_PORT}")
        print(f"发件人: {EMAIL_SENDER}, 收件人: {EMAIL_RECEIVER}")
        
        # 使用try-except捕获更详细的错误
        try:
            # 先尝试使用SMTP连接
            with smtplib.SMTP(EMAIL_SMTP_SERVER, 587) as server:
                server.starttls()
                server.login(EMAIL_SENDER, EMAIL_PASSWORD)
                server.send_message(msg)
            print("邮件发送成功（使用SMTP+STARTTLS）")
        except Exception as e:
            print(f"SMTP+STARTTLS发送失败: {e}")
            # 如果失败，再尝试SMTP_SSL
            try:
                with smtplib.SMTP_SSL(EMAIL_SMTP_SERVER, 465) as server:
                    server.login(EMAIL_SENDER, EMAIL_PASSWORD)
                    server.send_message(msg)
                print("邮件发送成功（使用SMTP_SSL）")
            except Exception as e2:
                print(f"SMTP_SSL发送失败: {e2}")
                raise
        
        print("邮件发送成功")
        return True
    except Exception as e:
        print(f"邮件发送失败: {e}")
        return False

# 主函数
def main():
    print(f"开始运行基金布林带量化信号系统...")
    print(f"布林带窗口长度: {BB_WINDOW}")
    print(f"处理基金列表: {FUND_CODES}")
    
    all_results = []
    
    # 处理每只基金
    for fund_code in FUND_CODES:
        results = process_fund(fund_code)
        if results:
            all_results.extend(results)
    
    # 生成Excel文件
    excel_file = generate_excel(all_results)
    
    # 发送邮件
    send_email(excel_file)
    
    print("基金布林带量化信号系统运行完成")

if __name__ == "__main__":
    main()
