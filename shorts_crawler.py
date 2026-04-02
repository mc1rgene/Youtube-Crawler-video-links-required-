import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException, 
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException
)
import pandas as pd
import time
import random
import re
from datetime import datetime, timedelta
import os
from functools import wraps
import requests
# 关闭SSL警告
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def measure_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"{func.__name__} 执行时间: {execution_time:.2f} 秒")
        return result
    return wrapper

class YouTubeShortsScraper:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.proxy_url = 'http://127.0.0.1:7890'  # 代理端口
        self.setup_driver()
        self.all_comments = []  # 存储所有视频的评论（批量爬取）
        # ========== 核心修改1：改为链接列表，可填入多个目标URL ==========
        self.target_urls = [
            # 在这里添加更多你要爬取的Shorts链接，示例：
            # "https://www.youtube.com/shorts/XXXXXXXXXXX",
            # "https://www.youtube.com/shorts/YYYYYYYYYYY"
        ]
        self.seen_comment_ids = set()  # 评论去重（单个视频内去重）
        self.seen_button_ids = set()   # 按钮去重（单个视频内去重）

    def check_proxy(self):
        """检查代理"""
        try:
            session = requests.Session()
            session.verify = False
            response = session.get(
                'https://www.youtube.com',
                proxies={'http': self.proxy_url, 'https': self.proxy_url},
                timeout=15
            )
            if response.status_code == 200:
                print("✅ 代理连接正常")
                return True
            else:
                print(f"⚠️ 代理响应异常，状态码: {response.status_code}")
                return True
        except Exception as e:
            print(f"⚠️ 代理检查失败: {e}，仍尝试继续运行")
            return True

    @measure_time
    def setup_driver(self):
        """初始化驱动"""
        self.check_proxy()
        
        os.environ['HTTP_PROXY'] = self.proxy_url
        os.environ['HTTPS_PROXY'] = self.proxy_url
        os.environ['NO_PROXY'] = 'localhost,127.0.0.1,127.0.0.0/8,::1'
        
        chrome_options = uc.ChromeOptions()
        chrome_options.add_argument(f'--proxy-server={self.proxy_url}')
        
        # 核心优化
        chrome_options.add_argument('--disable-background-timer-throttling')
        chrome_options.add_argument('--disable-backgrounding-occluded-windows')
        chrome_options.add_argument('--disable-renderer-backgrounding')
        chrome_options.add_argument('--disable-features=TranslateUI')
        chrome_options.add_argument('--disable-notifications')
        chrome_options.add_argument('--disable-popup-blocking')
        chrome_options.add_argument('--disable-web-security')
        chrome_options.add_argument('--allow-running-insecure-content')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--ignore-ssl-errors')
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--enable-images')
        
        # 随机UA
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
        ]
        chrome_options.add_argument(f'user-agent={random.choice(user_agents)}')
        
        try:
            if self.driver:
                self.driver.quit()
            self.driver = uc.Chrome(options=chrome_options,
            version_main=143,  # 欺骗驱动，让它认为浏览器是 143（3.5.5 支持的最高版本）
            suppress_welcome=True,  # 关闭版本不匹配的警告，强制启动
            executable_path=None  # 保持默认，让驱动自动查找 Chrome 可执行文件)
            )
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.driver.set_page_load_timeout(30)
            self.driver.set_script_timeout(30)
            self.wait = WebDriverWait(
                self.driver, 20, poll_frequency=1,
                ignored_exceptions=[StaleElementReferenceException, NoSuchElementException]
            )
            time.sleep(3)
            print("✅ 驱动初始化成功")
        except Exception as e:
            print(f"❌ 驱动初始化失败: {e}")
            raise

    def random_sleep(self, min_time=0.5, max_time=1.5):
        """随机等待"""
        time.sleep(random.uniform(min_time, max_time))

    def extract_absolute_time(self, time_element):
        """提取绝对时间"""
        try:
            absolute_time = time_element.get_attribute("title")
            if absolute_time and absolute_time.strip():
                if re.match(r'\d{4}-\d{2}-\d{2}', absolute_time):
                    return absolute_time.split('T')[0]
                elif re.match(r'\d{1,2}/\d{1,2}/\d{4}', absolute_time):
                    return datetime.strptime(absolute_time, '%m/%d/%Y').strftime('%Y-%m-%d')
                elif re.match(r'\w{3} \d{1,2}, \d{4}', absolute_time):
                    return datetime.strptime(absolute_time, '%b %d, %Y').strftime('%Y-%m-%d')
            relative_time = time_element.text.strip()
            return self.convert_relative_time(relative_time)
        except Exception as e:
            print(f"提取绝对时间失败: {e}")
            return self.convert_relative_time(time_element.text.strip())

    def convert_relative_time(self, relative_time):
        """相对时间转绝对时间"""
        try:
            if not relative_time or relative_time.strip() == "":
                return "未获取到"
            if "just now" in relative_time.lower() or "刚刚" in relative_time:
                return datetime.now().strftime("%Y-%m-%d")
            
            match = re.search(r'(\d+)\s*([smhdwmozy]+)', relative_time, re.IGNORECASE)
            if not match:
                return relative_time.strip()
            
            number = int(match.group(1))
            unit = match.group(2).lower()
            unit_map = {
                's': 'seconds', 'm': 'minutes', 'h': 'hours', 'd': 'days',
                'w': 'weeks', 'mo': 'months', 'y': 'years', 'z': 'years'
            }
            unit = unit_map.get(unit, None)
            if not unit:
                return relative_time.strip()
            
            now = datetime.now()
            if unit == 'months':
                delta = timedelta(days=number*30)
            elif unit == 'years':
                delta = timedelta(days=number*365)
            else:
                delta = timedelta(**{unit: number})
            return (now - delta).strftime("%Y-%m-%d")
        except Exception as e:
            print(f"转换相对时间失败: {e}")
            return "转换失败"

    def expand_all_replies(self):
        """中文按钮识别+不点击隐藏回复"""
        try:
            print("\n🔓 开始展开所有回复按钮（适配中文界面）...")
            expand_attempts = 0
            max_attempts = 150
            no_new_buttons = 0
            self.seen_button_ids.clear()

            while expand_attempts < max_attempts and no_new_buttons < 5:
                # 优先定位【显示更多回复】（中文）
                more_buttons = self.driver.find_elements(
                    By.XPATH,
                    '''//button[
                        (contains(@aria-label, "显示更多回复") or contains(@aria-label, "Show more replies"))
                        and not(contains(@aria-label, "隐藏") or contains(@aria-label, "Hide"))
                    ]'''
                )
                # 定位【X条回复】（中文）
                count_buttons = self.driver.find_elements(
                    By.XPATH,
                    '''//button[
                        (contains(@aria-label, "条回复") or contains(@aria-label, "replies"))
                        and not(contains(@aria-label, "显示更多") or contains(@aria-label, "Show more"))
                        and not(contains(@aria-label, "隐藏") or contains(@aria-label, "Hide"))
                    ]'''
                )

                # 合并按钮，优先处理"显示更多回复"
                all_buttons = more_buttons + count_buttons
                new_buttons = []
                
                # 按钮去重
                for btn in all_buttons:
                    btn_id = btn.get_attribute('id') or str(btn.location)
                    if btn_id not in self.seen_button_ids and btn.is_displayed() and btn.is_enabled():
                        new_buttons.append(btn)
                        self.seen_button_ids.add(btn_id)

                if not new_buttons:
                    no_new_buttons += 1
                    print(f"⚠️ 未找到新回复按钮（连续次数：{no_new_buttons}）")
                else:
                    no_new_buttons = 0
                    print(f"🔍 找到 {len(new_buttons)} 个回复按钮（优先显示更多回复）")
                    
                    # 逐个点击有效按钮
                    for btn in new_buttons:
                        try:
                            self.driver.execute_script(
                                "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                                btn
                            )
                            self.random_sleep(0.5, 1)
                            self.driver.execute_script("arguments[0].click();", btn)
                            btn_label = btn.get_attribute('aria-label') or "未知回复按钮"
                            print(f"✅ 点击：{btn_label}")
                            self.random_sleep(0.8, 1.5)
                        except Exception as e:
                            print(f"⚠️ 点击按钮失败: {e}")
                            continue
                
                # 滚动加载更多按钮
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                self.random_sleep(1.5, 2.5)
                expand_attempts += 1
            
            # 最终加载
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            print("✅ 所有回复按钮展开完成（未点击隐藏回复）")
        except Exception as e:
            print(f"展开回复失败: {e}")

    def parse_single_comment(self, comment_elem, comment_type="主评论", parent_author="", current_url=""):
        """解析单条评论（新增current_url参数，绑定当前视频链接）"""
        try:
            comment_id = comment_elem.get_attribute("id") or f"comment_{random.randint(1000, 9999)}"
            if comment_id in self.seen_comment_ids:
                return None

            # 评论作者
            author_elem = comment_elem.find_elements(By.CSS_SELECTOR, "#author-text span")
            author = author_elem[0].text.strip() if author_elem else "匿名用户"
            
            # 评论内容
            content_elem = comment_elem.find_elements(By.CSS_SELECTOR, "#content-text")
            comment_text = content_elem[0].text.strip() if content_elem else ""
            if not comment_text:
                return None
            
            # 评论时间
            time_elem = comment_elem.find_elements(By.CSS_SELECTOR, "#header-author .published-time-text a")
            if not time_elem:
                time_elem = comment_elem.find_elements(By.CSS_SELECTOR, "#header-author .published-time-text span")
            
            relative_time = ""
            absolute_time = "未获取到"
            if time_elem:
                relative_time = time_elem[0].text.strip()
                absolute_time = self.extract_absolute_time(time_elem[0])
            
            # 点赞数
            like_elem = comment_elem.find_elements(By.CSS_SELECTOR, "#vote-count-middle")
            likes = like_elem[0].text.strip() if like_elem else "0"
            likes = re.sub(r'[^\d,]', '', likes).replace(',', '') if likes else "0"
            
            self.seen_comment_ids.add(comment_id)
            return {
                '评论ID': comment_id,
                '评论类型': comment_type,
                '评论作者': author,
                '父评论作者': parent_author,
                '评论内容': comment_text,
                '评论时间(相对)': relative_time,
                '评论时间(绝对)': absolute_time,
                '评论点赞数': likes,
                '视频链接': current_url,  # 绑定当前视频链接，区分不同视频
                '视频类型': 'YouTube Shorts'
            }
        except Exception as e:
            print(f"解析单条评论失败: {e}")
            return None

    def parse_all_comments(self, current_url):
        """解析单个视频的所有主评论+二级评论（传入当前视频链接）"""
        try:
            print("\n📝 开始解析当前视频的所有评论（含二级）...")
            single_video_comments = []  # 存储单个视频的评论
            self.seen_comment_ids.clear()
            
            # 1. 解析主评论
            main_comments = self.driver.find_elements(By.CSS_SELECTOR, "ytd-comment-thread-renderer")
            print(f"🔍 找到 {len(main_comments)} 条主评论")
            
            for idx, main_comment in enumerate(main_comments):
                try:
                    if idx % 50 == 0 and idx > 0:
                        print(f"🔢 已解析 {idx} 条主评论，累计 {len(single_video_comments)} 条总评论")
                    
                    # 解析主评论
                    main_data = self.parse_single_comment(main_comment, "主评论", "", current_url)
                    if main_data:
                        single_video_comments.append(main_data)
                        main_author = main_data['评论作者']
                        
                        # 2. 解析二级回复
                        reply_containers = main_comment.find_elements(By.CSS_SELECTOR, "ytd-comment-replies-renderer")
                        for reply_container in reply_containers:
                            reply_comments = reply_container.find_elements(By.CSS_SELECTOR, "ytd-comment-renderer")
                            for reply_comment in reply_comments:
                                reply_data = self.parse_single_comment(reply_comment, "二级评论", main_author, current_url)
                                if reply_data:
                                    single_video_comments.append(reply_data)
                except Exception as e:
                    print(f"解析主评论 {idx} 失败: {e}")
                    continue
            
            # 去重校验（单个视频内去重）
            unique_comments = []
            seen_ids = set()
            for comment in single_video_comments:
                if comment['评论ID'] not in seen_ids:
                    seen_ids.add(comment['评论ID'])
                    unique_comments.append(comment)
            
            print(f"\n✅ 当前视频解析完成 - 总评论数（去重）: {len(unique_comments)}")
            print(f"- 主评论数: {len([c for c in unique_comments if c['评论类型'] == '主评论'])}")
            print(f"- 二级评论数: {len([c for c in unique_comments if c['评论类型'] == '二级评论'])}")
            
            # 返回当前视频的唯一评论列表
            return unique_comments
        except Exception as e:
            print(f"解析所有评论失败: {e}")
            return []

    def load_all_comments(self):
        """【核心修复】自动持续下滑加载全部评论（直到无更多内容）"""
        print("\n🔄 开始持续下滑加载所有评论...")
        # 初始化变量
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        no_new_content_count = 0  # 连续无新内容次数
        max_no_new_count = 8      # 连续8次无新内容则停止
        total_scrolls = 0         # 累计下滑次数
        last_comment_count = 0    # 上一次的主评论数
        
        while no_new_content_count < max_no_new_count:
            try:
                # 1. 下滑到页面底部（触发评论加载）
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                total_scrolls += 1
                # 等待评论加载（加长等待时间，确保内容加载完成）
                time.sleep(3)  # 固定等待，比随机更稳定
                
                # 2. 点击"加载更多"按钮（如果存在）
                try:
                    load_more_btn = self.driver.find_element(By.CSS_SELECTOR, "ytd-continuation-item-renderer")
                    if load_more_btn.is_displayed() and load_more_btn.is_enabled():
                        self.driver.execute_script("arguments[0].click();", load_more_btn)
                        print(f"📌 点击加载更多按钮（第{total_scrolls}次下滑）")
                        # 点击后额外等待，确保新评论加载
                        time.sleep(4)
                except (NoSuchElementException, StaleElementReferenceException):
                    pass  # 没有加载更多按钮则跳过
                
                # 3. 检查页面高度和评论数变化
                current_height = self.driver.execute_script("return document.body.scrollHeight")
                current_comment_count = len(self.driver.find_elements(By.CSS_SELECTOR, "ytd-comment-thread-renderer"))
                
                # 4. 判断是否加载到新内容
                if current_height == last_height and current_comment_count == last_comment_count:
                    no_new_content_count += 1
                    print(f"⚠️ 未加载到新评论（连续次数：{no_new_content_count}/{max_no_new_count}）| 当前主评论数：{current_comment_count}")
                else:
                    no_new_content_count = 0  # 重置计数
                    print(f"✅ 加载到新评论 | 主评论数：{last_comment_count} → {current_comment_count} | 页面高度：{last_height} → {current_height}")
                    last_height = current_height
                    last_comment_count = current_comment_count
                
                # 5. 防止无限循环（最大下滑次数保护）
                if total_scrolls >= 200:  # 最多下滑200次，可根据需要调整
                    print(f"⚠️ 已达到最大下滑次数（{total_scrolls}次），停止加载")
                    break
                
            except Exception as e:
                print(f"⚠️ 下滑加载出错: {e}，继续尝试...")
                no_new_content_count += 1
                continue
        
        print(f"\n✅ 持续加载完成 | 累计下滑 {total_scrolls} 次 | 最终主评论数：{last_comment_count}")

    def prompt_manual_click_comment(self, current_url):
        """手动点击评论按钮（显示当前视频链接，方便确认）"""
        print("\n" + "="*50)
        print("📌 请手动操作：")
        print(f"1. 确认页面是当前目标视频：{current_url}")
        print("2. 点击【评论】按钮（对话框图标），等待评论区展开")
        print("3. 回到终端按回车键继续...")
        print("="*50)
        input()
        self.random_sleep(5, 7)
        print("✅ 继续执行后续流程")

    # ========== 核心修改2：新增单个视频爬取方法 ==========
    def scrape_single_short(self, target_url):
        """爬取单个Shorts视频的评论（独立处理，避免跨视频数据污染）"""
        retry_count = 0
        max_retries = 2
        single_video_comments = []
        
        while retry_count < max_retries:
            try:
                print(f"\n{'='*60}")
                print(f"🌐 开始处理第 {self.target_urls.index(target_url)+1} 个视频：{target_url}")
                print(f"{'='*60}")
                self.driver.get(target_url)
                self.random_sleep(3, 5)
                
                # 检查人机验证
                verification = self.driver.find_elements(By.CSS_SELECTOR, "iframe[src*='recaptcha']")
                if verification:
                    print("⚠️ 检测到人机验证，请手动完成后等待...")
                    while verification:
                        time.sleep(2)
                        verification = self.driver.find_elements(By.CSS_SELECTOR, "iframe[src*='recaptcha']")
                    print("✅ 验证完成")
                
                # 手动点击评论按钮（传入当前视频链接）
                self.prompt_manual_click_comment(target_url)
                
                # 滚动到评论区
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.9);")
                self.random_sleep(2, 3)
                
                # 等待评论区加载
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ytd-comment-thread-renderer")))
                
                # 持续加载所有评论
                self.load_all_comments()
                
                # 展开所有二级回复
                self.expand_all_replies()
                
                # 再次加载，确保回复展开后加载剩余评论
                self.load_all_comments()
                
                # 解析当前视频的所有评论（传入当前视频链接）
                single_video_comments = self.parse_all_comments(target_url)
                break  # 爬取成功，退出重试循环
                
            except WebDriverException as e:
                print(f"⚠️ 驱动错误: {e}，当前视频重试 ({retry_count+1}/{max_retries})")
                retry_count += 1
                time.sleep(5)
                self.driver.refresh()
            except Exception as e:
                print(f"⚠️ 当前视频爬取错误: {e}，重试 ({retry_count+1}/{max_retries})")
                retry_count += 1
        
        if not single_video_comments:
            print(f"❌ 该视频多次重试失败，无有效评论数据返回")
        return single_video_comments

    # ========== 核心修改3：批量爬取所有视频 ==========
    @measure_time
    def scrape_batch_shorts(self):
        """批量爬取所有目标Shorts视频的评论"""
        if not self.target_urls:
            print("❌ 目标视频链接列表为空，请先填入有效URL")
            return []
        
        # 遍历所有视频链接，逐个爬取
        for target_url in self.target_urls:
            single_video_comments = self.scrape_single_short(target_url)
            # 将当前视频的评论合并到总列表
            self.all_comments.extend(single_video_comments)
            # 单个视频处理完成后，短暂等待，避免反爬
            print(f"\n⏳ 单个视频处理完成，等待5秒后继续下一个视频...")
            time.sleep(5)
        
        print(f"\n{'='*60}")
        print(f"✅ 所有视频批量处理完成！")
        print(f"{'='*60}")
        return self.all_comments

    def save_comments(self, comments, output_folder="output"):
        """保存批量爬取的所有评论到CSV（区分不同视频）"""
        if not comments:
            print("⚠️ 无评论数据可保存")
            return
        
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # 批量爬取文件名标识
        filename = f"{output_folder}/YouTube_Shorts评论_批量爬取版_{timestamp}.csv"
        
        df = pd.DataFrame(comments)
        columns_order = [
            '评论ID', '评论类型', '评论作者', '父评论作者',
            '评论内容', '评论时间(相对)', '评论时间(绝对)',
            '评论点赞数', '视频链接', '视频类型'
        ]
        df = df.reindex(columns=columns_order, fill_value="")
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"\n💾 所有视频评论已保存到: {filename}")
        print(f"📊 批量爬取统计信息:")
        print(f"- 总评论数（所有视频，去重）: {len(comments)}")
        # 按视频链接分组统计
        video_group = df.groupby('视频链接').size()
        for video_url, comment_count in video_group.items():
            print(f"- 视频 {video_url[-11:]}（后11位ID）: {comment_count} 条评论")
        
        # 额外统计评论类型
        print(f"- 总主评论数: {len([c for c in comments if c['评论类型'] == '主评论'])}")
        print(f"- 总二级评论数: {len([c for c in comments if c['评论类型'] == '二级评论'])}")
        
        valid_times = [c['评论时间(绝对)'] for c in comments if c['评论时间(绝对)'] not in ["未获取到", "转换失败"]]
        if valid_times:
            print(f"- 有效绝对时间数: {len(valid_times)}")
            print(f"- 最早评论时间: {min(valid_times)}")
            print(f"- 最晚评论时间: {max(valid_times)}")

    def close(self):
        """关闭浏览器"""
        if self.driver:
            try:
                self.driver.quit()
                print("\n✅ 浏览器已关闭")
            except:
                pass

@measure_time
def main():
    scraper = YouTubeShortsScraper()
    try:
        # ========== 核心修改4：调用批量爬取方法 ==========
        all_comments = scraper.scrape_batch_shorts()
        scraper.save_comments(all_comments)
    except Exception as e:
        print(f"\n❌ 程序执行出错: {e}")
        if scraper.all_comments:
            scraper.save_comments(scraper.all_comments)
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
