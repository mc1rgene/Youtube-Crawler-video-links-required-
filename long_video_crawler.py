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

class YouTubeLongVideoScraper:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.proxy_url = 'http://127.0.0.1:7890'  # 代理端口
        self.scraped_videos = set()  # 已爬取视频去重
        self.verification_times = []  # 人机验证统计
        self.all_comments = []  # 存储所有视频的完整评论（含二级）
        # 楼中楼爬取必备：去重集合
        self.seen_comment_ids = set()  # 评论去重（单个视频内）
        self.seen_button_ids = set()   # 回复按钮去重（单个视频内）
        self.setup_driver()

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
        """初始化驱动（已修复uc冲突配置，延长超时，减少渲染压力）"""
        self.check_proxy()
        
        os.environ['HTTP_PROXY'] = self.proxy_url
        os.environ['HTTPS_PROXY'] = self.proxy_url
        os.environ['NO_PROXY'] = 'localhost,127.0.0.1,127.0.0.0/8,::1'
        
        chrome_options = uc.ChromeOptions()
        chrome_options.add_argument(f'--proxy-server={self.proxy_url}')
        
        # 核心反爬配置（无uc冲突）
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
        
        # ========== 新增：减少渲染压力（不加载非必要资源，降低卡顿） ==========
        chrome_options.add_argument('--disable-images')  # 禁用图片加载（评论爬取无需图片）
        chrome_options.add_argument('--disable-video-autoplay')  # 禁用视频自动播放
        chrome_options.add_argument('--disable-media-stream')  # 禁用媒体流
        chrome_options.add_argument('--disable-plugins')  # 禁用插件（广告、Flash等）
        chrome_options.add_argument('--disable-software-rasterizer')  # 禁用软件光栅化，减少CPU占用
        
        # 随机UA
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
        ]
        chrome_options.add_argument(f'user-agent={random.choice(user_agents)}')
        
        try:
            if self.driver:
                self.driver.quit()
            self.driver = uc.Chrome(
                options=chrome_options,
                version_main=144,  # 适配你的Chrome版本144.0.7559.110
                suppress_welcome=True,
                executable_path=None
            )
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # ========== 核心修改：延长各类超时时间（适配代理慢加载环境） ==========
            self.driver.set_page_load_timeout(60)  # 页面加载超时从30→60秒
            self.driver.set_script_timeout(60)    # 脚本执行超时从30→60秒
            self.wait = WebDriverWait(
                self.driver, 40, poll_frequency=1,  # 元素等待超时从20→40秒
                ignored_exceptions=[StaleElementReferenceException, NoSuchElementException]
            )
            time.sleep(3)
            print("✅ 驱动初始化成功（已配置代理，延长超时，减少渲染压力）")
        except Exception as e:
            print(f"❌ 驱动初始化失败: {e}")
            raise

    def random_sleep(self, min_time=0.5, max_time=1.5):
        """随机等待"""
        time.sleep(random.uniform(min_time, max_time))

    # ========== 新增：提取YouTube视频ID（兼容多种链接格式） ==========
    def extract_video_id(self, url):
        """
        从YouTube链接中提取11位视频ID，兼容常见格式：
        1. https://www.youtube.com/watch?v=XXXXXXXXXXX
        2. https://youtu.be/XXXXXXXXXXX
        3. https://www.youtube.com/watch?v=XXXXXXXXXXX&t=10s
        """
        try:
            # 匹配 youtu.be 格式
            if 'youtu.be/' in url:
                return url.split('youtu.be/')[1].split('?')[0][:11]
            # 匹配 watch?v= 格式
            elif 'v=' in url:
                return url.split('v=')[1].split('&')[0][:11]
            else:
                return None
        except Exception as e:
            print(f"⚠️ 提取视频ID失败：{url}，错误：{e}")
            return None

    # ========== 新增：校验YouTube链接有效性 ==========
    def is_valid_youtube_url(self, url):
        """校验是否为有效的YouTube视频链接"""
        video_id = self.extract_video_id(url)
        return video_id is not None and len(video_id) == 11

    # ========== 优化：时间提取（适配楼中楼评论时间） ==========
    def extract_absolute_time(self, time_element):
        """提取绝对时间（优先获取title属性，再转换相对时间）"""
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
        """相对时间转绝对时间（适配中英文）"""
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

    # ========== 核心：展开所有楼中楼回复按钮（优化后减少元素扰动） ==========
    def expand_all_replies(self):
        """展开所有回复按钮（适配中英文界面，减少页面元素扰动，避免后续解析失效）"""
        try:
            print("\n🔓 开始展开所有楼中楼回复按钮（适配中文/英文界面）...")
            expand_attempts = 0
            max_attempts = 100  # 减少最大尝试次数，降低页面扰动
            no_new_buttons = 0
            self.seen_button_ids.clear()

            while expand_attempts < max_attempts and no_new_buttons < 5:
                # 1. 优先定位【显示更多回复】（中文）/【Show more replies】（英文）
                more_buttons = self.driver.find_elements(
                    By.XPATH,
                    '''//button[
                        (contains(@aria-label, "显示更多回复") or contains(@aria-label, "Show more replies"))
                        and not(contains(@aria-label, "隐藏") or contains(@aria-label, "Hide"))
                    ]'''
                )
                # 2. 定位【X条回复】（中文）/【X replies】（英文）
                count_buttons = self.driver.find_elements(
                    By.XPATH,
                    '''//button[
                        (contains(@aria-label, "条回复") or contains(@aria-label, "replies"))
                        and not(contains(@aria-label, "显示更多") or contains(@aria-label, "Show more"))
                        and not(contains(@aria-label, "隐藏") or contains(@aria-label, "Hide"))
                    ]'''
                )

                # 3. 合并按钮，优先处理"显示更多回复"
                all_buttons = more_buttons + count_buttons
                new_buttons = []
                
                # 4. 按钮去重（避免重复点击同一按钮）
                for btn in all_buttons:
                    try:
                        # 提前判断按钮是否有效，避免缓存失效按钮
                        if not btn.is_displayed() or not btn.is_enabled():
                            continue
                        btn_id = btn.get_attribute('id') or str(btn.location)
                        if btn_id not in self.seen_button_ids:
                            new_buttons.append(btn)
                            self.seen_button_ids.add(btn_id)
                    except StaleElementReferenceException:
                        continue

                if not new_buttons:
                    no_new_buttons += 1
                    print(f"⚠️ 未找到新回复按钮（连续次数：{no_new_buttons}）")
                else:
                    no_new_buttons = 0
                    print(f"🔍 找到 {len(new_buttons)} 个待展开回复按钮（优先显示更多回复）")
                    
                    # 5. 逐个点击有效按钮（用js点击，减少页面扰动）
                    for btn in new_buttons:
                        try:
                            self.driver.execute_script(
                                "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                                btn
                            )
                            self.random_sleep(0.5, 1)
                            self.driver.execute_script("arguments[0].click();", btn)
                            btn_label = btn.get_attribute('aria-label') or "未知回复按钮"
                            print(f"✅ 点击展开：{btn_label[:50]}...")  # 截断过长标签
                            self.random_sleep(0.8, 1.5)
                        except StaleElementReferenceException:
                            print(f"⚠️ 按钮已失效，跳过该按钮")
                            continue
                        except Exception as e:
                            print(f"⚠️ 点击按钮失败: {e}")
                            continue
                
                # 6. 缓慢滚动加载更多按钮，减少DOM刷新
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                self.random_sleep(1.5, 2.5)
                expand_attempts += 1
            
            # 7. 最终稳定等待，让页面DOM完全渲染（核心：解决元素过时的关键步骤）
            print("\n⏳ 等待页面DOM稳定，避免评论元素失效...")
            time.sleep(8)
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            print("✅ 所有楼中楼回复按钮展开完成，页面已稳定")
        except Exception as e:
            print(f"展开楼中楼回复失败: {e}")

    # ========== 【核心优化】解析单条评论（增加重试机制，解决元素过时问题） ==========
    def parse_single_comment(self, comment_elem, comment_type="主评论", parent_author="", current_url="", max_retries=2):
        """解析单条评论（增加重试机制，处理元素过时异常）"""
        retry_count = 0
        while retry_count < max_retries:
            try:
                # 提前判断元素是否有效
                if not comment_elem or not comment_elem.is_displayed():
                    return None

                # 1. 评论去重ID
                comment_id = comment_elem.get_attribute("id") or f"comment_{random.randint(1000, 9999)}"
                if comment_id in self.seen_comment_ids:
                    return None

                # 2. 评论作者（重新获取元素，不缓存）
                author_elem = comment_elem.find_elements(By.CSS_SELECTOR, "#author-text span")
                author = author_elem[0].text.strip() if (author_elem and author_elem[0].is_displayed()) else "匿名用户"
                
                # 3. 评论内容（重新获取元素，不缓存）
                content_elem = comment_elem.find_elements(By.CSS_SELECTOR, "#content-text")
                comment_text = content_elem[0].text.strip() if (content_elem and content_elem[0].is_displayed()) else ""
                if not comment_text:
                    return None
                
                # 4. 评论时间（优化提取，适配楼中楼，处理元素过时）
                time_elem = comment_elem.find_elements(By.CSS_SELECTOR, "#header-author .published-time-text a")
                if not time_elem:
                    time_elem = comment_elem.find_elements(By.CSS_SELECTOR, "#header-author .published-time-text span")
                
                relative_time = ""
                absolute_time = "未获取到"
                if time_elem and time_elem[0].is_displayed():
                    relative_time = time_elem[0].text.strip()
                    absolute_time = self.extract_absolute_time(time_elem[0])
                
                # 5. 点赞数（重新获取元素，不缓存）
                like_elem = comment_elem.find_elements(By.CSS_SELECTOR, "#vote-count-middle")
                likes = like_elem[0].text.strip() if (like_elem and like_elem[0].is_displayed()) else "0"
                likes = re.sub(r'[^\d,]', '', likes).replace(',', '') if likes else "0"
                
                # 6. 标记已解析，避免重复
                self.seen_comment_ids.add(comment_id)
                
                # 7. 返回评论数据
                return {
                    '评论ID': comment_id,
                    '评论类型': comment_type,
                    '评论作者': author,
                    '父评论作者': parent_author,
                    '评论内容': comment_text,
                    '评论时间(相对)': relative_time,
                    '评论时间(绝对)': absolute_time,
                    '评论点赞数': likes,
                    '视频链接': current_url,
                    '视频类型': 'YouTube Long Video'
                }
            except StaleElementReferenceException:
                retry_count += 1
                print(f"⚠️ 评论元素过时，重试第 {retry_count}/{max_retries} 次")
                self.random_sleep(1, 2)
                # 重试时重新获取评论元素（核心：避免复用失效元素）
                try:
                    comment_elem = self.driver.find_element(By.ID, comment_elem.get_attribute("id"))
                except:
                    continue
            except Exception as e:
                print(f"解析单条评论失败: {e}")
                return None
        print(f"❌ 评论元素多次重试仍失效，跳过该评论")
        return None

    # ========== 【核心修改】手动加载确认（提示用户操作） ==========
    def wait_for_manual_load_confirm(self):
        """提示用户手动下滑加载评论区，等待终端回车确认"""
        print("\n" + "="*70)
        print("📢 请进行以下操作：")
        print("1. 回到浏览器窗口，找到视频的评论区")
        print("2. 手动反复下滑页面，直到所有需要的评论都加载完成（无新评论加载为止）")
        print("3. 加载完成后，回到终端，按【回车】键继续执行后续操作（楼中楼展开+解析）")
        print("="*70)
        # 等待用户终端按回车
        input("🔍 等待你的确认（加载完成后按回车）...")
        print("\n✅ 收到你的确认，开始执行后续操作！")
        time.sleep(2)

    # ========== 【核心优化】解析所有评论（分批获取，避免批量元素失效） ==========
    def parse_all_comments(self, current_url):
        """解析单个视频的所有评论（分批获取元素，处理过时异常，含楼中楼）"""
        try:
            print("\n📝 开始解析当前视频的所有评论（含楼中楼二级评论）...")
            single_video_comments = []
            self.seen_comment_ids.clear()
            
            # 1. 分批获取主评论容器（避免一次性获取过多导致批量失效）
            main_comment_selector = By.CSS_SELECTOR, "ytd-comment-thread-renderer"
            self.wait.until(EC.presence_of_all_elements_located(main_comment_selector))
            main_comment_count = len(self.driver.find_elements(*main_comment_selector))
            print(f"🔍 找到 {main_comment_count} 条主评论容器，开始分批解析")
            
            # 2. 遍历解析主评论（逐个重新获取，不缓存批量元素）
            for idx in range(main_comment_count):
                try:
                    # 核心：每次循环都重新获取当前主评论元素，避免复用失效引用
                    main_comments = self.driver.find_elements(*main_comment_selector)
                    if idx >= len(main_comments):
                        break
                    main_comment = main_comments[idx]
                    
                    if idx % 50 == 0 and idx > 0:
                        print(f"🔢 已解析 {idx} 条主评论，累计 {len(single_video_comments)} 条总评论")
                        # 每解析50条，等待页面稳定，减少元素扰动
                        time.sleep(2)
                    
                    # 2.1 解析主评论（带重试机制）
                    main_data = self.parse_single_comment(main_comment, "主评论", "", current_url)
                    if main_data:
                        single_video_comments.append(main_data)
                        main_author = main_data['评论作者']
                        
                        # 2.2 解析楼中楼二级评论（逐个获取回复容器，避免缓存）
                        reply_containers = main_comment.find_elements(By.CSS_SELECTOR, "ytd-comment-replies-renderer")
                        for reply_container in reply_containers:
                            try:
                                reply_comments = reply_container.find_elements(By.CSS_SELECTOR, "ytd-comment-renderer")
                                for reply_comment in reply_comments:
                                    reply_data = self.parse_single_comment(reply_comment, "二级评论（楼中楼）", main_author, current_url)
                                    if reply_data:
                                        single_video_comments.append(reply_data)
                            except StaleElementReferenceException:
                                print(f"⚠️ 回复容器元素过时，跳过该主评论的楼中楼")
                                continue
                except StaleElementReferenceException:
                    print(f"⚠️ 主评论元素过时，跳过第 {idx} 条主评论")
                    continue
                except Exception as e:
                    print(f"解析主评论 {idx} 失败: {e}")
                    continue
            
            # 3. 去重校验（单个视频内避免重复数据）
            unique_comments = []
            seen_ids = set()
            for comment in single_video_comments:
                if comment['评论ID'] not in seen_ids:
                    seen_ids.add(comment['评论ID'])
                    unique_comments.append(comment)
            
            # 4. 统计当前视频评论数据
            main_comment_count_final = len([c for c in unique_comments if c['评论类型'] == '主评论'])
            reply_comment_count_final = len([c for c in unique_comments if c['评论类型'] == '二级评论（楼中楼）'])
            print(f"\n✅ 当前视频解析完成 - 总评论数（去重）: {len(unique_comments)}")
            print(f"- 主评论数: {main_comment_count_final}")
            print(f"- 楼中楼二级评论数: {reply_comment_count_final}")
            
            return unique_comments
        except Exception as e:
            print(f"解析所有评论（含楼中楼）失败: {e}")
            return []

    # ========== 优化：获取视频评论（整合手动确认+稳定展开+解析，增加容错） ==========
    @measure_time
    def get_video_comments(self, url):
        """获取单个视频的完整评论（含楼中楼，整合「手动加载确认」+稳定展开+解析流程，增加超时容错）"""
        try:
            print(f"\n⏳ 正在加载视频页面：{url[-11:]}，请耐心等待（代理环境可能耗时较长）...")
            self.driver.get(url)
            
            # 延长初始等待，让页面核心框架加载完成
            self.random_sleep(5, 8)
            
            # 1. 检查人机验证，完成后额外等待页面稳定
            verification = self.driver.find_elements(By.CSS_SELECTOR, "iframe[src*='recaptcha']")
            if verification:
                print("⚠️ 检测到人机验证，请手动完成后等待...")
                while verification:
                    time.sleep(2)
                    verification = self.driver.find_elements(By.CSS_SELECTOR, "iframe[src*='recaptcha']")
                print("✅ 人机验证完成，继续等待页面稳定...")
                self.random_sleep(3, 5)  # 验证完成后额外等待，恢复页面稳定
            
            # 2. 滚动到评论区附近，方便用户手动下滑，延长滚动后等待
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.7);")
            self.random_sleep(4, 6)
            
            # 3. 容错等待评论区元素，超时后刷新重试一次
            try:
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ytd-comment-thread-renderer")))
            except TimeoutException:
                print("⚠️ 首次等待评论区元素超时，刷新页面后重试...")
                self.driver.refresh()
                self.random_sleep(5, 8)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.7);")
                self.random_sleep(4, 6)
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ytd-comment-thread-renderer")))
            
            print("\n✅ 已定位到评论区，准备进入手动加载阶段")
            
            # 4. 【关键步骤】等待用户手动下滑加载，终端回车确认
            self.wait_for_manual_load_confirm()
            
            # 5. 核心：展开所有楼中楼回复按钮（优化后减少元素扰动）
            self.expand_all_replies()
            
            # 6. 解析所有评论（含楼中楼，分批获取避免元素失效）
            single_video_comments = self.parse_all_comments(url)
            
            return single_video_comments
        except Exception as e:
            print(f"获取视频评论（含楼中楼）失败: {e}")
            return []

    def scrape_video(self, url):
        """爬取单个长视频的评论（含楼中楼，带重试机制，延长重试间隔）"""
        # 先校验链接有效性
        if not self.is_valid_youtube_url(url):
            print(f"❌ 无效的YouTube链接：{url}，跳过该视频")
            return []
        
        # 提取视频ID去重
        video_id = self.extract_video_id(url)
        if video_id in self.scraped_videos:
            print(f"⚠️ 视频 {video_id} 已爬取过，跳过该视频")
            return []
        
        retry_count = 0
        max_retries = 2
        single_video_comments = []
        
        while retry_count < max_retries:
            try:
                single_video_comments = self.get_video_comments(url)
                # 标记为已爬取
                self.scraped_videos.add(video_id)
                break
            except WebDriverException as e:
                print(f"⚠️ 驱动错误: {e}，当前视频重试 ({retry_count+1}/{max_retries})")
                retry_count += 1
                time.sleep(10)  # 延长重试间隔从5→10秒，让驱动恢复
                self.driver.refresh()
            except Exception as e:
                print(f"⚠️ 当前视频爬取错误: {e}，重试 ({retry_count+1}/{max_retries})")
                retry_count += 1
                time.sleep(10)
        
        if not single_video_comments:
            print(f"❌ 该视频多次重试失败，无有效评论数据返回")
        return single_video_comments

    def check_verification(self):
        """检查并等待人机验证"""
        try:
            start_time = time.time()
            verification_elements = self.driver.find_elements(By.CSS_SELECTOR, "iframe[src*='recaptcha']")
            if verification_elements:
                print("检测到人机验证，等待验证完成...")
                while verification_elements:
                    time.sleep(1)
                    verification_elements = self.driver.find_elements(By.CSS_SELECTOR, "iframe[src*='recaptcha']")
                end_time = time.time()
                verification_time = end_time - start_time
                self.verification_times.append(verification_time)
                print(f"人机验证完成，耗时: {verification_time:.2f} 秒")
                return True
            return False
        except Exception as e:
            print(f"检查人机验证时出错: {e}")
            return False

    # ========== 优化：保存评论（含楼中楼字段，统计详细信息） ==========
    def save_comments(self, comments, output_folder="output"):
        """保存爬取的所有评论（含楼中楼，格式规整，统计详细信息）"""
        if not comments:
            print("⚠️ 无评论数据可保存")
            return
        
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{output_folder}/YouTube_长视频评论_含楼中楼_{timestamp}.csv"
        
        # 转换为DataFrame，规整字段顺序
        df = pd.DataFrame(comments)
        columns_order = [
            '评论ID', '评论类型', '评论作者', '父评论作者',
            '评论内容', '评论时间(相对)', '评论时间(绝对)',
            '评论点赞数', '视频链接', '视频类型'
        ]
        df = df.reindex(columns=columns_order, fill_value="")
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        # 打印详细统计信息
        print(f"\n💾 所有评论数据已保存到: {filename}")
        print(f"📊 爬取统计信息:")
        print(f"- 总评论数（所有视频，去重）: {len(comments)}")
        print(f"- 总主评论数: {len([c for c in comments if c['评论类型'] == '主评论'])}")
        print(f"- 总楼中楼二级评论数: {len([c for c in comments if c['评论类型'] == '二级评论（楼中楼）'])}")
        
        # 按视频链接分组统计
        video_group = df.groupby('视频链接').size()
        if len(video_group) > 0:
            print(f"- 有效视频数: {len(video_group)}")
            for video_url, comment_count in video_group.head(10).items():  # 只打印前10个，避免过长
                video_id = self.extract_video_id(video_url) or video_url[-11:]
                print(f"  - 视频 {video_id}: {comment_count} 条评论")
        
        # 时间统计
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

# ========== 核心修改：手动提供视频链接，替换关键词搜索 ==========
@measure_time
def main():
    scraper = YouTubeLongVideoScraper()
    try:
        # ========== 【手动填写视频链接】==========
        # 已放入你提供的去重后链接，直接运行即可
        video_urls = [
            
        ]
        
        # 校验并过滤有效链接
        valid_video_urls = [url for url in video_urls if scraper.is_valid_youtube_url(url)]
        if not valid_video_urls:
            print(f"❌ 无有效YouTube视频链接，程序终止")
            return
        
        print(f"\n{'='*60}")
        print(f"🌐 共找到 {len(valid_video_urls)} 个有效YouTube视频链接，开始逐个爬取")
        print(f"{'='*60}")
        
        # 逐个爬取视频评论（含楼中楼）
        for i, url in enumerate(valid_video_urls, 1):
            video_id = scraper.extract_video_id(url)
            print(f"\n{'-'*50}")
            print(f"处理第 {i}/{len(valid_video_urls)} 个视频: {video_id}")
            print(f"视频链接: {url}")
            print(f"{'-'*50}")
            
            single_video_comments = scraper.scrape_video(url)
            scraper.all_comments.extend(single_video_comments)
            
            # 延长等待，降低反爬风险（爬取楼中楼时更重要）
            print(f"\n⏳ 单个视频处理完成，等待5秒后继续下一个视频...")
            time.sleep(5)
        
        # 保存所有含楼中楼的评论数据
        scraper.save_comments(scraper.all_comments)
    
    except Exception as e:
        print(f"\n❌ 程序执行出错: {e}")
        # 即使报错，保存已爬取的数据
        if scraper.all_comments:
            scraper.save_comments(scraper.all_comments)
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
