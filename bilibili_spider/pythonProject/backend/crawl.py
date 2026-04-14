import requests
import time
import pandas as pd
from xml.etree import ElementTree as ET
import os
import shutil
from pathlib import Path

# ---------------------- 配置项 ----------------------
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Cookie": "buvid3=FB14C817-CB52-1A99-8552-BFD76EC553A563494infoc; b_nut=1764637263; _uuid=104B10D1B4-411C-5E104-3B105-76109136A15B263004infoc; buvid_fp=ed1e56132a41e4763c9a4171d98fd7a0; home_feed_column=5; buvid4=40F4D44E-5D1B-29E2-3DE2-430A70FAAE1G66315-025120209-WIgxV4KnPzYa/X0E5F/0WA%3D%3D; CURRENT_QUALITY=0; rpdid=|(klRmkk)l)|0J'u~YRYJ~lYm; DedeUserID=291913457; DedeUserID__ckMd5=74ee3250d2ae1593; theme-tip-show=SHOWED; bili_ticket=eyJhbGciOiJIUzI1NiIsImtpZCI6InMwMyIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NjU0MTkyMTAsImlhdCI6MTc2NTE1OTk1MCwicGx0IjotMX0.E9MQ_hRFNh2r_8N-DB4_n-xMajzlAktQAQjEA2Ojsoc; bili_ticket_expires=1765419150; SESSDATA=5364b744%2C1780712013%2C47c4e%2Ac2CjB66YaUIyoX8rI3rA296WeAyIVq6xHWUhEasCpg4ILOsCMpM5lCs8Q7YgHExfxyW90SVlIyMTV1dnZ3dDUtMENoRlZzdlhUTmxMbEE1UXJxUndtTjVlTjhleV9jbFJSMUFCcUZqdnE1bWJQSUR6RXZEVVNDSThCR19feWFSRXE1cVpfanhHRmt3IIEC; bili_jct=79bbf3575b6f61474fe85d134a0544f2; b_lsid=8C10625D4_19AFEBE75A6; browser_resolution=2560-1271; sid=51850a4r; bp_t_offset_291913457=1144059586915860480; CURRENT_FNVAL=2000"
}

# 单视频抓取时，每个视频抓多少
max_comments = 5000
max_danmaku = 2000

# 基础路径 & 输出文件夹
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
OUTPUT_DIR = os.path.join(BASE_DIR, "output")


def prepare_output_dir():
    """确保 output 文件夹存在，并在每次运行前清空其中内容。"""
    if os.path.exists(OUTPUT_DIR):
        for name in os.listdir(OUTPUT_DIR):
            full_path = os.path.join(OUTPUT_DIR, name)
            if os.path.isfile(full_path) or os.path.islink(full_path):
                os.remove(full_path)
            elif os.path.isdir(full_path):
                shutil.rmtree(full_path)
    else:
        os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------------------- 工具函数 ----------------------
def get_aid(bvid):
    """
    通过 bvid 获取 aid。

    若请求失败或返回非 JSON，则返回 None。
    """
    url = f'https://api.bilibili.com/x/web-interface/view?bvid={bvid}'
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        if res.status_code != 200:
            print(f"获取 aid 请求失败，HTTP {res.status_code}")
            return None
        data = res.json()
        return data.get('data', {}).get('aid')
    except Exception as e:
        print(f"获取 aid 发生异常: {e}")
        return None


def get_cid(bvid):
    """
    通过 bvid 获取 cid（第一页分 P 的 cid）。
    如果请求失败则返回 None。
    """
    url = f'https://api.bilibili.com/x/player/pagelist?bvid={bvid}'
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        if res.status_code != 200:
            print(f"获取 cid 请求失败，HTTP {res.status_code}")
            return None
        data = res.json()
        pages = data.get('data')
        if pages:
            return pages[0]['cid']
    except Exception as e:
        print(f"获取 cid 发生异常: {e}")
    return None


def get_comments(bvid, max_comments):
    """
    获取单个视频的评论：返回列表，每个元素为 dict：
    { 'content': 文本, 'uname': 昵称, 'mid': 用户ID }

    请求异常或接口返回异常时提前终止。
    """
    comment_list = []
    page = 1
    aid = get_aid(bvid)
    if not aid:
        print("获取 aid 失败，无法抓取评论。")
        return comment_list

    while len(comment_list) < max_comments:
        url = 'https://api.bilibili.com/x/v2/reply'
        params = {
            'type': 1,
            'oid': aid,
            'pn': page
        }
        try:
            res = requests.get(url, headers=HEADERS, params=params, timeout=10)
            if res.status_code != 200:
                print(f"评论接口请求失败，HTTP {res.status_code}")
                break
            data = res.json()
            replies = data.get('data', {}).get('replies')
        except Exception as e:
            print(f"获取评论发生异常: {e}")
            break

        if not replies:
            break

        for reply in replies:
            msg = reply.get('content', {}).get('message', '')
            member = reply.get('member', {}) or {}
            uname = member.get('uname')
            mid = member.get('mid')
            # 评论时间戳（秒）
            ctime = reply.get('ctime')
            comment_list.append({
                'content': msg,
                'uname': uname,
                'mid': mid,
                'ctime': ctime
            })
            if len(comment_list) >= max_comments:
                break

        page += 1
        time.sleep(1)

    return comment_list


def get_danmaku(bvid, max_danmaku):
    """
    获取单个视频的弹幕：返回列表，每个元素为 dict：
    { 'content': 文本, 'user_hash': midHash }

    若请求或解析失败则返回空列表。
    """
    danmakus = []
    cid = get_cid(bvid)
    if not cid:
        print("未获取到 cid，跳过弹幕。")
        return danmakus

    url = f'https://api.bilibili.com/x/v1/dm/list.so?oid={cid}'
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        if res.status_code != 200:
            print(f"弹幕接口请求失败，HTTP {res.status_code}")
            return danmakus
        res.encoding = 'utf-8'
        root = ET.fromstring(res.text)
    except Exception as e:
        print(f"获取弹幕发生异常: {e}")
        return danmakus

    for d in root.findall('d'):
        text = d.text or ""
        p = d.get('p', '')
        fields = p.split(',')
        user_hash = fields[6] if len(fields) > 6 else None
        # 弹幕参数 p 格式: 
        # 第 1 个字段为出现时间（单位秒），第 5 个字段为时间戳（Unix 秒）
        rel_time = None
        unix_time = None
        if fields:
            try:
                rel_time = float(fields[0])
            except Exception:
                rel_time = None
            if len(fields) > 4:
                try:
                    unix_time = float(fields[4])
                except Exception:
                    unix_time = None
        danmakus.append({
            'content': text,
            'user_hash': user_hash,
            'rel_time': rel_time,
            'unix_time': unix_time
        })
        if len(danmakus) >= max_danmaku:
            break

    return danmakus


def extract_bvid(s: str) -> str:
    """
    从用户输入中提取 BV 号：
    - 如果是链接，就从中截取 'BV' 开头的 12 位
    - 如果用户直接输 BV 号，就原样返回
    """
    s = s.strip()
    if "BV" in s:
        idx = s.find("BV")
        return s[idx:idx+12]
    return s


# ---------------------- 单视频抓取逻辑 ----------------------
def crawl_single_video(bvid: str):
    """针对单个视频抓取评论和弹幕，并分别保存 CSV。"""
    print(f"目标视频 BV 号：{bvid}")

    print("正在抓取评论……")
    comments = get_comments(bvid, max_comments)
    print(f"评论抓取完成，共 {len(comments)} 条。")

    print("正在抓取弹幕……")
    danmakus = get_danmaku(bvid, max_danmaku)
    print(f"弹幕抓取完成，共 {len(danmakus)} 条。")

    comment_data = []
    danmaku_data = []

    for c in comments:
        comment_data.append({
            'bvid': bvid,
            'content': c.get('content'),
            'uname': c.get('uname'),
            'mid': c.get('mid'),
            'ctime': c.get('ctime')
        })

    for d in danmakus:
        danmaku_data.append({
            'bvid': bvid,
            'content': d.get('content'),
            'user_hash': d.get('user_hash'),
            'rel_time': d.get('rel_time'),
            'unix_time': d.get('unix_time')
        })

    # 分别保存 CSV
    if comment_data:
        df_c = pd.DataFrame(comment_data)
        comments_path = os.path.join(OUTPUT_DIR, f"bilibili_{bvid}_comments.csv")
        df_c.to_csv(comments_path, index=False, encoding='utf-8-sig')
        print(f"评论数据已保存：{comments_path}")
    else:
        print("没抓到任何评论，不生成评论 CSV。")

    if danmaku_data:
        df_d = pd.DataFrame(danmaku_data)
        danmaku_path = os.path.join(OUTPUT_DIR, f"bilibili_{bvid}_danmaku.csv")
        df_d.to_csv(danmaku_path, index=False, encoding='utf-8-sig')
        print(f"弹幕数据已保存：{danmaku_path}")
    else:
        print("没抓到任何弹幕，不生成弹幕 CSV。")

def run_crawl(bv: str, job_id: str, cookie: str = None) -> str:
    base = Path("output")
    base.mkdir(parents=True, exist_ok=True)

    for p in base.glob("*"):
        if p.is_file():
            p.unlink()

    if cookie:
        HEADERS["Cookie"] = cookie

    global OUTPUT_DIR
    OUTPUT_DIR = str(base)

    crawl_single_video(bv)

    return str(base.resolve())

# ---------------------- 主执行逻辑 ----------------------
def main():
    # 每次运行先清空 output 文件夹
    prepare_output_dir()
    print(f"已清空输出目录：{OUTPUT_DIR}")

    url_or_bvid = input("请输入 B站视频链接或BV号：").strip()
    bvid = extract_bvid(url_or_bvid)

    if not bvid.startswith("BV"):
        print("未识别出合法的 BV 号，请检查输入。")
        return

    crawl_single_video(bvid)
    print("单视频数据抓取流程结束。")


if __name__ == '__main__':
    main()
