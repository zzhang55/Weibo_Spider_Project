import requests
import re
import csv
import os
import json
import shutil
from datetime import datetime
import time
import random

# 基础URL（不带参数）
base_url = 'https://m.weibo.cn/api/container/getIndex'

# 初始请求参数
params = {
    'luicode': '10000011',
    'lfid': '231583',
    'launchid': '10000360-page_H5',
    'type': 'uid',
    'value': 'YOUR_UID_HERE',           # 【请修改】这里填博主的UID
    'containerid': '107603YOUR_UID_HERE', # 【请修改】这里填 107603 + UID
    'since_id': ''                        # 留空表示从第一页开始抓取
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    'Referer': 'https://m.weibo.cn/',     # 通用 Referer
    'Accept': 'application/json, text/plain, */*',
    'X-Requested-With': 'XMLHttpRequest',
}

# 微博Cookie（必须填写）
# 获取方法：
# 1. 电脑浏览器按 F12 打开开发者工具
# 2. 访问或刷新 m.weibo.cn
# 3. 在 Network 面板找到任意请求，查看 Request Headers
# 4. 找到 Cookie 字段，复制 "SUB=xxxxxxx;" 这一段即可
cookies = {
    'SUB': 'YOUR_COOKIE_HERE', # 【请修改】你的微博 SUB Cookie
}

def strip_html(text):
    return re.sub(r'<[^>]*?>', '', text or '')

def format_date_for_csv(created_at):
    """将微博的 created_at 格式转换为CSV显示格式：2025/7/16 Mon 14:21"""
    try:
        # 微博日期格式：Mon Jun 16 14:21:37 +0800 2025
        dt = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y')
        # 转换为 2025/7/16 Mon 14:21 格式
        # %Y/%m/%d 会生成 2025/07/16，需要手动处理月份和日期去掉前导零
        year = dt.year
        month = dt.month  # 已经是数字，不需要转换
        day = dt.day
        weekday = dt.strftime('%a')  # Mon, Tue, Wed...
        hour = dt.hour
        minute = dt.minute
        return f'{year}/{month}/{day} {weekday} {hour:02d}:{minute:02d}'
    except Exception as e:
        # 如果解析失败，使用当前时间作为备用
        print(f'日期解析失败: {created_at}, 使用备用格式')
        now = datetime.now()
        return f'{now.year}/{now.month}/{now.day} {now.strftime("%a")} {now.hour:02d}:{now.minute:02d}'

def format_date_for_folder(created_at):
    """将微博的 created_at 格式转换为文件夹名格式 YYYY-MM-DD_HH-MM"""
    try:
        # 微博日期格式：Mon Jun 16 14:21:37 +0800 2025
        dt = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y')
        # 转换为 YYYY-MM-DD_HH-MM 格式
        return dt.strftime('%Y-%m-%d')
    except Exception as e:
        # 如果解析失败，使用当前时间戳作为备用
        print(f'日期解析失败: {created_at}, 使用备用名称')
        return datetime.now().strftime('%Y-%m-%d')

def download_images(pic_urls, base_date_prefix):
    """下载图片到 images 根目录，文件名格式：YYYY-MM-DD_01.jpg"""
    if not pic_urls:
        return 0
    
    success_count = 0
    
    for pic_url in pic_urls:
        # 寻找可用的文件名序号
        idx = 1
        while True:
            filename = f'{base_date_prefix}_{idx:02d}.jpg'
            file_path = os.path.join(images_base_path, filename)
            if not os.path.exists(file_path):
                break
            idx += 1
            
        print(f'  准备下载: {filename} ...')

        # 每个图片尝试重试3次
        for attempt in range(3):
            try:
                # 下载图片，设置15秒超时
                img_response = requests.get(pic_url, headers=headers, timeout=15)
                img_response.raise_for_status()
                
                # 保存图片
                with open(file_path, 'wb') as f:
                    f.write(img_response.content)
                
                success_count += 1
                print(f'  ✅ 已下载图片: {filename}')
                break # 成功则跳出重试循环
            except Exception as e:
                if attempt < 2:
                    print(f'  ⚠️ 图片下载失败，正在重试 ({attempt+1}/3)...')
                    time.sleep(1)
                else:
                    print(f'  ❌ 图片下载最终失败: {filename}, 错误: {e}')
        # 继续下一个图片
    
    return success_count

def download_video(video_url, base_date_prefix):
    """下载视频到 images 根目录"""
    if not video_url:
        return None
    
    # 寻找可用的文件名序号
    idx = 1
    while True:
        filename = f'{base_date_prefix}_{idx:02d}.mp4'
        file_path = os.path.join(images_base_path, filename)
        if not os.path.exists(file_path):
            break
        idx += 1
    
    relative_path = os.path.join(os.path.basename(images_base_path), filename)
        
    print(f'  准备下载视频: {filename} ...')
    try:
        # 视频较大，使用流式下载
        with requests.get(video_url, headers=headers, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print(f'  ✅ 视频下载完成: {filename}')
        return relative_path
    except Exception as e:
        print(f'  ❌ 视频下载失败: {e}')
        # 尝试删除可能损坏的文件
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass
        return ''

csv_path = os.path.join(os.path.dirname(__file__), 'weibo_data.csv')
images_base_path = os.path.join(os.path.dirname(__file__), 'images')

# 确保 images 文件夹存在
os.makedirs(images_base_path, exist_ok=True)

# 内存查重集合
seen_ids = set()

# 从CSV文件中读取已存在的微博ID，避免重复抓取
def load_existing_ids():
    """从CSV文件中读取已存在的微博ID"""
    if not os.path.exists(csv_path):
        return set()
    
    existing_ids = set()
    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            # 跳过表头
            header = next(reader, None)
            if header is None:
                return set()
            
            # 检查CSV格式：新格式有4列（包含ID），旧格式有3列
            has_id_column = len(header) == 4 and header[0] == '微博ID'
            
            if has_id_column:
                # 新格式：直接读取ID列
                for row in reader:
                    if len(row) > 0 and row[0]:  # ID列不为空
                        existing_ids.add(row[0])
            else:
                # 旧格式：需要从数据中提取ID（暂时无法提取，因为旧CSV没有ID）
                # 这种情况下，seen_ids保持为空，但会在本次运行中记录
                pass
    except Exception as e:
        print(f'读取已存在ID时出错: {e}')
    
    return existing_ids

# 加载已存在的ID
seen_ids = load_existing_ids()
initial_count = len(seen_ids)  # 记录初始数量
if seen_ids:
    print(f'已加载 {len(seen_ids)} 条已存在的微博ID，将跳过重复抓取')

# 写入表头（仅在文件不存在或为空时写）
if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['微博ID', '发布时间', '微博文案', '图片链接', '视频链接'])
    except PermissionError:
        print('❌ 错误: 无法创建CSV文件！')
        print(f'   文件路径: {csv_path}')
        print('   可能原因: 文件正在被其他程序占用')
        print('   解决方案: 请关闭所有使用该文件的程序，然后重新运行')
        exit(1)
elif os.path.exists(csv_path):
    # 检查是否需要升级CSV格式（从3列升级到4列）
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header and len(header) == 3:
            # 旧格式，需要升级
            print('检测到旧格式CSV，正在升级格式（添加微博ID列）...')
            rows = list(reader)
            try:
                # 备份旧文件
                backup_path = csv_path + '.backup'
                shutil.copy2(csv_path, backup_path)
                print(f'已备份旧文件到: {backup_path}')
                # 写入新格式
                with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f)
                    writer.writerow(['微博ID', '发布时间', '微博文案', '图片链接'])
                    # 旧数据没有ID，留空
                    for row in rows:
                        writer.writerow([''] + row)
                print('CSV格式升级完成')
            except PermissionError:
                print('❌ 错误: 无法升级CSV文件格式！')
                print(f'   文件路径: {csv_path}')
                print('   可能原因: 文件正在被Excel或其他程序打开')
                print('   解决方案: 请关闭所有使用该文件的程序，然后重新运行')
                exit(1)

# 检查CSV文件是否可写（避免文件被占用导致后续写入失败）
def check_csv_writable():
    """检查CSV文件是否可写"""
    if not os.path.exists(csv_path):
        return True
    try:
        # 尝试以追加模式打开文件
        with open(csv_path, 'a', encoding='utf-8-sig'):
            pass
        return True
    except PermissionError:
        return False
    except Exception:
        return False

if not check_csv_writable():
    print('❌ 错误: CSV文件无法写入！')
    print(f'   文件路径: {csv_path}')
    print('   可能原因: 文件正在被Excel或其他程序打开')
    print('   解决方案: 请关闭所有使用该文件的程序（如Excel），然后重新运行程序')
    exit(1)


def find_and_store_mblogs(obj):
    if isinstance(obj, dict):
        # 1. 检查当前字典是不是一个 mblog
        if 'created_at' in obj and 'text' in obj:
            store_mblog(obj)
            return  #如果是微博对象，处理完就返回，不需深入（除非微博里套微博，通常不需要）
        
        # 2. 如果不是，继续遍历它的所有值
        for v in obj.values():
            find_and_store_mblogs(v)
            
    elif isinstance(obj, list):
        for item in obj:
            find_and_store_mblogs(item)

def store_mblog(mblog):
    # 获取唯一标识：优先使用 id，如果没有则使用 mid
    blog_id = mblog.get('id') or mblog.get('mid')
    if not blog_id:
        return  # 如果没有ID，跳过
    
    # 即使已存在也继续处理，以便补全下载失败的图片
    # if blog_id in seen_ids:
    #     return  # 已存在，跳过
    
    raw_date = mblog.get('created_at', '')
    # 格式化日期为CSV显示格式：2025/7/16 Mon 14:21
    formatted_date = format_date_for_csv(raw_date)
    clean_text = strip_html(mblog.get('text', ''))
    
    # 提取图片URL列表
    pic_urls = []
    for pic in mblog.get('pics', []):
        if isinstance(pic, dict):
            if 'large' in pic and 'url' in pic['large']:
                pic_urls.append(pic['large']['url'])
            elif 'url' in pic:
                pic_urls.append(pic['url'])
    
    # 提取视频URL
    video_url = ''
    page_info = mblog.get('page_info', {})
    if page_info and page_info.get('type') == 'video':
        media_info = page_info.get('media_info', {})
        # 优先通过 stream_url, mp4_720p_mp4, mp4_hd_url 获取
        video_url = media_info.get('mp4_720p_mp4') or media_info.get('mp4_hd_url') or media_info.get('stream_url')

    # 生成基础日期前缀 e.g. 2024-09-20
    date_prefix = format_date_for_folder(raw_date)
    # 记录该微博的图片文件夹路径现在就是 images 文件夹本身
    folder_relative_path = 'images'
    
    # 下载图片
    if pic_urls:
        print(f'Checking/Downloading images for date: {date_prefix}')
        download_images(pic_urls, date_prefix)

    # 下载视频
    video_relative_path = ''
    if video_url:
        print(f'Found video, downloading for date: {date_prefix}')
        video_relative_path = download_video(video_url, date_prefix)
    
    # 写入CSV（仅当ID不在seen_ids中时才写入）
    if blog_id not in seen_ids:
        try:
            with open(csv_path, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                # 写入5列：ID, 时间, 文案, 图片路径, 视频路径
                writer.writerow([blog_id, formatted_date, clean_text, folder_relative_path, video_relative_path])
            
            seen_ids.add(blog_id)  # 添加到已见集合
            print(f'✅ 已成功入库：[{clean_text[:10]}]...')
        except PermissionError:
            print(f'❌ 无法写入CSV文件: {csv_path}')
            print('   错误原因: 文件被其他程序占用（可能正在Excel或其他编辑器中打开）')
            print('   解决方案: 请关闭Excel或其他正在使用该文件的程序，然后重新运行')
            print(f'   当前微博信息: ID={blog_id}, 时间={formatted_date}, 文案={clean_text[:20]}...')
            # 不添加seen_ids，这样下次运行时会重新尝试写入
            return
        except Exception as e:
            print(f'❌ 写入CSV文件时出错: {e}')
            print(f'   文件路径: {csv_path}')
            return
    else:
        # 如果已存在，仅打印一条简短的日志，表示跳过CSV写入但检查了图片
        # print(f'   (ID已存在，跳过CSV写入，已检查图片补全)')
        pass

while True:
    print(f'Requesting weibo data (since_id={params.get("since_id")})...')
    
    # 网络请求重试循环
    while True:
        try:
            response = requests.get(base_url, headers=headers, cookies=cookies, params=params, timeout=15)
            print(f'Status Code: {response.status_code}')
            break # 成功请求，跳出重试循环
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
            print(f'⚠️ Network Error (SSL/Connection) occurred: {e}')
            print('   Waiting 30 seconds before retrying...')
            time.sleep(30)
        except Exception as e:
            print(f'❌ Unexpected Error: {e}')
            # 其他错误暂时跳出，交由外层处理或直接退出
            response = None
            break

    # 如果请求彻底失败且response未定义或为空
    if 'response' not in locals() or not response:
         print('❌ Request failed after retries. Stopping.')
         break

    try:        
        if response.status_code != 200:
            print(f'❌ Request failed, Status Code: {response.status_code}')
            print(f'Response Content: {response.text[:500]}')
            break
            
        data = response.json()
        if data.get('ok') != 1:
            print('❌ Data status is not OK (ok != 1)')
            # 有时候虽然ok!=1但仍有数据，这里视情况而定，通常意味着出错或结束
            if 'msg' in data:
                print(f'Message: {data["msg"]}')
            # 尝试继续解析或退出
            # break 
            
        print('✅ JSON data retrieved successfully')
        
    except Exception as e:
        print(f'❌ Request or Parsing failed: {e}')
        # 出错后暂停一下重试，或者直接退出
        break



    find_and_store_mblogs(data)
    
    # 获取下一页的since_id
    cardlistInfo = data.get('data', {}).get('cardlistInfo', {})
    since_id = cardlistInfo.get('since_id')
    
    if since_id:
        print(f'Detected next page, since_id: {since_id}')
        params['since_id'] = since_id  # 更新参数，准备抓取下一页
        
        # 随机暂停 2-5 秒，避免请求过快
        sleep_time = random.uniform(3, 6)
        print(f'Waiting {sleep_time:.2f} seconds before next request...')
        time.sleep(sleep_time)
    else:
        print('No more pages (since_id not found). stopping...')
        break

# 输出统计信息
new_count = len(seen_ids) - initial_count
print('\n' + '='*50)
print('Execution completed！')
print(f'New weibo count: {new_count}')
print(f'Total weibo count: {len(seen_ids)}')
print('='*50)
