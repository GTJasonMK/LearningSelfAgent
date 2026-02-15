import requests
import csv
import json
from datetime import datetime, timedelta
import time
import sys

def fetch_sina_gold_price():
    """尝试从新浪财经获取黄金价格数据"""
    # 使用更可能有效的合约代码
    url = "https://hq.sinajs.cn/list=GC00Y"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://finance.sina.com.cn/'
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            content = resp.text
            print(f"新浪原始响应: {content[:200]}")
            # 尝试解析价格
            # 格式示例: var hq_str_GC00Y="黄金连续,1920.5,1921.0,...";
            if '=' in content:
                data_str = content.split('=')[1].strip('\" ;')
                parts = data_str.split(',')
                if len(parts) > 1:
                    # 假设第二个字段是价格（美元/盎司）
                    price_usd_per_oz = float(parts[1])
                    # 转换为元/克: 1盎司=31.1035克, 汇率假设6.8
                    price_cny_per_g = price_usd_per_oz * 6.8 / 31.1035
                    return [{
                        'date': datetime.now().strftime('%Y-%m-%d'),
                        'price_cny_per_g': round(price_cny_per_g, 2)
                    }]
    except Exception as e:
        print(f"新浪请求失败: {e}")
    return None

def fetch_goldorg_data():
    """尝试从 gold.org 的公开API获取数据"""
    # 使用 gold.org 的公开API（示例端点，可能需要调整）
    url = "https://data-asg.goldprice.org/dbXRates/CNY"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            print(f"gold.org 响应: {json.dumps(data)[:200]}")
            # 解析结构
            if 'items' in data:
                items = data['items']
                result = []
                for item in items:
                    if 'xauPrice' in item and 'timestamp' in item:
                        # 价格是美元/盎司，转换为元/克
                        price_usd_per_oz = item['xauPrice']
                        price_cny_per_g = price_usd_per_oz * 6.8 / 31.1035
                        date_str = datetime.fromtimestamp(item['timestamp']/1000).strftime('%Y-%m-%d')
                        result.append({
                            'date': date_str,
                            'price_cny_per_g': round(price_cny_per_g, 2)
                        })
                return result
    except Exception as e:
        print(f"gold.org 请求失败: {e}")
    return None

def fetch_jin10_data():
    """尝试从金十数据获取（使用公开API）"""
    url = "https://flash-api.jin10.com/get_flash"  # 示例API，可能需要参数
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'X-App-Id': 'bVBF4FyRTn5NJF5n',
        'X-Version': '1.0.0'
    }
    params = {
        'channel': 'gold_realtime',
        'max_num': 100
    }
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            print(f"金十数据响应: {json.dumps(data)[:200]}")
            # 实际解析逻辑需根据API响应调整
            return None
    except Exception as e:
        print(f"金十请求失败: {e}")
    return None

def fetch_alternative_source():
    """尝试其他备用数据源"""
    # 尝试世界黄金协会的公开数据
    url = "https://www.gold.org/goldhub/api/gold-prices"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            print(f"GoldHub API 响应长度: {len(str(data))}")
            # 尝试解析
            if isinstance(data, list):
                result = []
                for entry in data[:90]:  # 最近90天
                    if 'date' in entry and 'price' in entry:
                        # 假设价格是美元/盎司
                        price_usd_per_oz = entry['price']
                        price_cny_per_g = price_usd_per_oz * 6.8 / 31.1035
                        result.append({
                            'date': entry['date'][:10],
                            'price_cny_per_g': round(price_cny_per_g, 2)
                        })
                return result
    except Exception as e:
        print(f"备用源请求失败: {e}")
    return None

def generate_fallback_data():
    """生成回退数据（仅当所有真实源都失败时使用，并明确标记）"""
    print("警告：所有真实数据源尝试失败，生成标记为示例的回退数据")
    data = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    current = start_date
    price = 500.0
    import random
    while current <= end_date:
        price += random.uniform(-5, 5)
        price = max(450, min(550, price))
        data.append({
            'date': current.strftime('%Y-%m-%d'),
            'price_cny_per_g': round(price, 2),
            'source': '示例数据（真实抓取失败）'
        })
        current += timedelta(days=1)
    return data

def main():
    print("开始获取最近三个月的黄金价格数据（元/克）...")
    
    # 尝试多个数据源
    sources = [
        fetch_goldorg_data,
        fetch_sina_gold_price,
        fetch_alternative_source,
        fetch_jin10_data
    ]
    
    real_data = None
    for source in sources:
        print(f"尝试数据源: {source.__name__}")
        data = source()
        if data:
            real_data = data
            print(f"成功从 {source.__name__} 获取 {len(data)} 条数据")
            break
        time.sleep(1)
    
    # 如果所有真实源都失败，使用回退数据（明确标记）
    if not real_data:
        print("所有真实数据源尝试失败，使用回退数据")
        real_data = generate_fallback_data()
    
    # 确保数据按日期排序
    real_data.sort(key=lambda x: x['date'])
    
    # 写入CSV
    csv_path = "gold_price.csv"
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        if real_data and len(real_data) > 0:
            fieldnames = list(real_data[0].keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(real_data)
            print(f"数据已写入 {csv_path}，共 {len(real_data)} 条记录")
        else:
            print("错误：无有效数据可写入")
            sys.exit(1)
    
    # 打印前几行预览
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()[:6]
            print("CSV预览:")
            for line in lines:
                print(line.rstrip())
    except Exception as e:
        print(f"读取CSV预览失败: {e}")

if __name__ == "__main__":
    main()