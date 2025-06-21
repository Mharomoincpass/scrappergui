import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import ttkbootstrap as ttkb
from ttkbootstrap.constants import *
from ttkbootstrap.style import Style
import time
import csv
import os
import requests
import re
import shutil
import glob
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import torch
from transformers import pipeline
from deep_translator import GoogleTranslator
from langdetect import detect
import webbrowser
import threading
from PIL import Image, ImageTk
import io

# Benchmark Lookup Table (in INR)
BENCHMARKS = {
    "Apparel": {"CPC": 27.00, "CTR": 0.0184, "ConvRate": 0.039, "AOV": 4500.00},
    "Finance & Insurance": {"CPC": 226.20, "CTR": 0.0111, "ConvRate": 0.0525, "AOV": 51000.00},
    "B2B SaaS": {"CPC": 151.20, "CTR": 0.0078, "ConvRate": 0.028, "AOV": 60000.00},
    "Healthcare": {"CPC": 79.20, "CTR": 0.0098, "ConvRate": 0.035, "AOV": 6600.00},
    "E-commerce": {"CPC": 39.00, "CTR": 0.0145, "ConvRate": 0.042, "AOV": 7200.00},
    "Education": {"CPC": 66.00, "CTR": 0.0085, "ConvRate": 0.03, "AOV": 12000.00},
    "Travel & Hospitality": {"CPC": 57.00, "CTR": 0.013, "ConvRate": 0.038, "AOV": 18000.00},
    "Real Estate": {"CPC": 100.80, "CTR": 0.009, "ConvRate": 0.025, "AOV": 90000.00},
    "Digital Marketing Services": {"CPC": 120.00, "CTR": 0.0100, "ConvRate": 0.035, "AOV": 30000.00},
    "IT Staffing & Recruitment": {"CPC": 90.00, "CTR": 0.0090, "ConvRate": 0.040, "AOV": 24000.00},
    "Software Training": {"CPC": 60.00, "CTR": 0.0090, "ConvRate": 0.035, "AOV": 15000.00},
    "Web Hosting & Domains": {"CPC": 80.00, "CTR": 0.0080, "ConvRate": 0.030, "AOV": 12000.00},
    "Freelance Software Development": {"CPC": 100.00, "CTR": 0.0085, "ConvRate": 0.032, "AOV": 36000.00},
    "Business Consulting & Services": {"CPC": 15.80, "CTR": 0.0121, "ConvRate": 0.025, "AOV": 300000.00},
    "Software Development": {"CPC": 80.00, "CTR": 0.009, "ConvRate": 0.035, "AOV": 30000.00}
}

# Default fallback metrics (in INR)
DEFAULT_CPC = 31.66
DEFAULT_CTR = 0.01
DEFAULT_CONVERSION_RATE = 0.02
DEFAULT_AOV = 60000
DEFAULT_ACTIVE_DAYS = 1.0
CONFIDENCE_THRESHOLD = 0.3
translation_cache = {}

# Initialize classifier with GPU/CPU handling
device = 0 if torch.cuda.is_available() else -1  # 0 for GPU, -1 for CPU
print(f"Using device: {'GPU' if device >= 0 else 'CPU'}")

try:
    classifier = pipeline(
        "zero-shot-classification",
        model="valhalla/distilbart-mnli-12-3",
        tokenizer="valhalla/distilbart-mnli-12-3",
        device=device
    )
    INDUSTRIES = list(BENCHMARKS.keys())
except Exception as e:
    print(f"⚠️ Error loading HuggingFace model: {e}. Falling back to default metrics.")
    classifier = None

class MediaPreviewWindow:
    def __init__(self, parent, media_files):
        self.top = tk.Toplevel(parent)
        self.top.title("Media Preview")
        self.top.geometry("600x400")
        self.media_files = media_files
        self.current_index = 0

        self.media_frame = ttkb.Frame(self.top, padding=10)
        self.media_frame.pack(fill=BOTH, expand=True)

        self.media_label = ttkb.Label(self.media_frame)
        self.media_label.pack(fill=BOTH, expand=True)

        nav_frame = ttkb.Frame(self.top)
        nav_frame.pack(fill=X, pady=5)
        ttkb.Button(nav_frame, text="Previous", command=self.show_previous).pack(side=LEFT, padx=5)
        ttkb.Button(nav_frame, text="Next", command=self.show_next).pack(side=LEFT, padx=5)
        ttkb.Button(nav_frame, text="Open File", command=self.open_file).pack(side=LEFT, padx=5)

        self.show_media()

    def show_media(self):
        if not self.media_files:
            self.media_label.config(text="No media available.")
            return

        file_path = self.media_files[self.current_index]
        ext = os.path.splitext(file_path)[1].lower()

        if ext in ('.jpg', '.jpeg', '.png', '.gif'):
            try:
                image = Image.open(file_path)
                image.thumbnail((500, 300))
                photo = ImageTk.PhotoImage(image)
                self.media_label.config(image=photo, text="")
                self.media_label.image = photo
            except Exception as e:
                self.media_label.config(text=f"Error loading image: {e}", image="")
        elif ext in ('.mp4', '.webm', '.ogv'):
            self.media_label.config(text=f"Video: {os.path.basename(file_path)}\n(Click 'Open File' to view)", image="")
        else:
            self.media_label.config(text=f"Unsupported file: {os.path.basename(file_path)}", image="")

    def show_previous(self):
        if self.current_index > 0:
            self.current_index -= 1
            self.show_media()

    def show_next(self):
        if self.current_index < len(self.media_files) - 1:
            self.current_index += 1
            self.show_media()

    def open_file(self):
        file_path = self.media_files[self.current_index]
        webbrowser.open(f"file://{os.path.abspath(file_path)}")

class MetaAdsScraperApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Meta Ads Scraper")
        self.style = Style(theme='darkly')
        self.root.geometry("1400x1000")
        self.root.resizable(True, True)
        
        self.keyword_var = tk.StringVar(value="Odint Consulting Services")
        self.country_var = tk.StringVar(value="ALL")
        self.scrolls_var = tk.StringVar(value="1")
        self.output_var = tk.StringVar(value="meta_ads_ranked.csv")
        self.media_folder_var = tk.StringVar(value="ad_media")
        self.debug_log_var = tk.StringVar(value="scrape_errors.csv")
        self.metrics_output_var = tk.StringVar(value="ad_metrics_estimates.csv")
        self.is_running = False
        self.ads_data = []
        self.metrics_data = []
        self.media_files = []
        
        # Split the screen into two parts using PanedWindow
        self.paned_window = ttkb.PanedWindow(self.root, orient=HORIZONTAL)
        self.paned_window.pack(fill=BOTH, expand=True)

        # Left pane for configuration, buttons, and progress
        self.left_pane = ttkb.Frame(self.paned_window)
        self.paned_window.add(self.left_pane, weight=1)

        # Right pane for top 5 ads
        self.right_pane = ttkb.Frame(self.paned_window)
        self.paned_window.add(self.right_pane, weight=1)

        # Left pane content
        config_frame = ttkb.LabelFrame(self.left_pane, text="Configuration", padding=10)
        config_frame.pack(fill=X, pady=5)
        
        ttkb.Label(config_frame, text="Keyword:").grid(row=0, column=0, padx=5, pady=5, sticky=W)
        ttkb.Entry(config_frame, textvariable=self.keyword_var, width=50).grid(row=0, column=1, padx=5, pady=5)
        
        ttkb.Label(config_frame, text="Country Code:").grid(row=1, column=0, padx=5, pady=5, sticky=W)
        country_combo = ttkb.Combobox(config_frame, textvariable=self.country_var, values=["ALL", "US", "UK", "IN", "CA", "AU"], width=10)
        country_combo.grid(row=1, column=1, padx=5, pady=5, sticky=W)
        
        ttkb.Label(config_frame, text="Number of Scrolls:").grid(row=2, column=0, padx=5, pady=5, sticky=W)
        ttkb.Entry(config_frame, textvariable=self.scrolls_var, width=10).grid(row=2, column=1, padx=5, pady=5, sticky=W)
        
        ttkb.Label(config_frame, text="Output CSV:").grid(row=3, column=0, padx=5, pady=5, sticky=W)
        ttkb.Entry(config_frame, textvariable=self.output_var, width=50).grid(row=3, column=1, padx=5, pady=5)
        
        ttkb.Label(config_frame, text="Media Folder:").grid(row=4, column=0, padx=5, pady=5, sticky=W)
        ttkb.Entry(config_frame, textvariable=self.media_folder_var, width=50).grid(row=4, column=1, padx=5, pady=5)
        ttkb.Button(config_frame, text="Browse", command=self.browse_media_folder).grid(row=4, column=2, padx=5, pady=5)
        
        button_frame = ttkb.Frame(self.left_pane)
        button_frame.pack(fill=X, pady=10)
        
        self.start_button = ttkb.Button(button_frame, text="Start Scraping", style="primary.TButton", command=self.start_scraping)
        self.start_button.pack(side=LEFT, padx=5)
        
        ttkb.Button(button_frame, text="Open Media Folder", style="secondary.TButton", command=self.open_media_folder).pack(side=LEFT, padx=5)
        
        ttkb.Button(button_frame, text="Clear Outputs", style="danger.TButton", command=self.clear_outputs).pack(side=LEFT, padx=5)
        
        progress_frame = ttkb.LabelFrame(self.left_pane, text="Progress", padding=10)
        progress_frame.pack(fill=BOTH, expand=True, pady=5)
        
        self.progress_text = tk.Text(progress_frame, height=10, width=50, wrap=WORD)
        self.progress_text.pack(fill=BOTH, expand=True, pady=5)
        self.progress_scroll = ttkb.Scrollbar(progress_frame, orient=VERTICAL, command=self.progress_text.yview)
        self.progress_text.config(yscrollcommand=self.progress_scroll.set)
        self.progress_scroll.pack(side=RIGHT, fill=Y)
        
        # Right pane content
        results_frame = ttkb.LabelFrame(self.right_pane, text="Top 5 Ads", padding=10)
        results_frame.pack(fill=BOTH, expand=True, pady=5)
        
        ttkb.Label(results_frame, text="Ads are ranked by activity duration (hours active).", font=("Helvetica", 10, "italic")).pack(anchor=W, pady=5)
        
        self.canvas = tk.Canvas(results_frame, highlightthickness=0)
        self.scrollbar = ttkb.Scrollbar(results_frame, orient=VERTICAL, command=self.canvas.yview)
        self.scrollable_frame = ttkb.Frame(self.canvas)
        
        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )
        
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        
        self.canvas.pack(side=LEFT, fill=BOTH, expand=True)
        self.scrollbar.pack(side=RIGHT, fill=Y)
        
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)
        
        self.log_message("Ready to start scraping. Enter configuration and click 'Start Scraping'.")

    def _on_mousewheel(self, event):
        self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def log_message(self, message):
        self.progress_text.insert(tk.END, f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {message}\n")
        self.progress_text.see(tk.END)
        self.root.update()

    def browse_media_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.media_folder_var.set(folder)
            self.log_message(f"Selected media folder: {folder}")

    def open_media_folder(self):
        folder = self.media_folder_var.get()
        if os.path.exists(folder):
            webbrowser.open(f"file://{os.path.abspath(folder)}")
            self.log_message(f"Opened media folder: {folder}")
        else:
            messagebox.showwarning("Warning", f"Media folder '{folder}' does not exist.")

    def clear_outputs(self):
        try:
            media_folder = self.media_folder_var.get()
            if os.path.exists(media_folder):
                shutil.rmtree(media_folder)
                self.log_message(f"Cleared media folder: {media_folder}")
            os.makedirs(media_folder, exist_ok=True)
            self.log_message(f"Created empty media folder: {media_folder}")
            
            csv_files = glob.glob("*.csv")
            for csv_file in csv_files:
                try:
                    os.remove(csv_file)
                    self.log_message(f"Deleted CSV file: {csv_file}")
                except Exception as e:
                    self.log_message(f"Error deleting CSV file {csv_file}: {e}")
                    
            for widget in self.scrollable_frame.winfo_children():
                widget.destroy()
            self.ads_data = []
            self.metrics_data = []
            self.media_files = []
            self.log_message("Cleared all outputs and reset results display.")
        except Exception as e:
            self.log_message(f"Error clearing outputs: {e}")
            messagebox.showerror("Error", f"Failed to clear outputs: {e}")

    def display_media(self, media_paths):
        if not media_paths:
            return None
        for path in media_paths:
            ext = os.path.splitext(path)[1].lower()
            if ext in ('.jpg', '.jpeg', '.png', '.gif'):
                try:
                    image = Image.open(path)
                    image.thumbnail((150, 150))
                    photo = ImageTk.PhotoImage(image)
                    return photo
                except Exception as e:
                    self.log_message(f"Error loading image {path}: {e}")
            elif ext in ('.mp4', '.webm', '.ogv'):
                return None
        return None

    def create_ad_card(self, index, ad, metric, media_paths):
        card_frame = ttkb.Frame(self.scrollable_frame, padding=15, style="Card.TFrame", relief=RAISED, borderwidth=2)
        card_frame.pack(fill=X, pady=20)
        
        card_frame.bind("<Enter>", lambda e: card_frame.configure(style="CardHover.TFrame"))
        card_frame.bind("<Leave>", lambda e: card_frame.configure(style="Card.TFrame"))
        
        media_container = ttkb.Frame(card_frame, padding=5, style="Media.TFrame")
        media_container.pack(side=LEFT, padx=10)
        
        media_label = ttkb.Label(media_container, text="No media", font=("Helvetica", 8, "italic"))
        if media_paths:
            photo = self.display_media(media_paths)
            if photo:
                media_label.config(image=photo)
                media_label.image = photo
            else:
                media_label.config(text="Video (Preview)")
        media_label.pack()
        
        details_frame = ttkb.Frame(card_frame, padding=5)
        details_frame.pack(side=LEFT, fill=X, expand=True)
        
        ttkb.Label(details_frame, text=f"Rank {index}: {metric['Advertiser']}", font=("Helvetica", 12, "bold")).pack(anchor=W)
        ttkb.Label(details_frame, text=ad["Ad Text"][:80] + ("..." if len(ad["Ad Text"]) > 80 else ""), wraplength=400, justify=LEFT, font=("Helvetica", 9)).pack(anchor=W, pady=2)
        
        ttkb.Label(details_frame, text=f"Ranked due to: {ad['Hours Active']:.2f} hours active", font=("Helvetica", 8, "italic"), foreground="gray").pack(anchor=W, pady=2)
        
        ttkb.Separator(details_frame, orient=HORIZONTAL).pack(fill=X, pady=5)
        
        ttkb.Label(details_frame, text="Metrics", font=("Helvetica", 10, "bold")).pack(anchor=W)
        
        metrics_frame = ttkb.Frame(details_frame)
        metrics_frame.pack(anchor=W, pady=2)
        
        ttkb.Label(metrics_frame, text=f"Industry: {metric['Industry']}", font=("Helvetica", 8)).pack(anchor=W)
        ttkb.Label(metrics_frame, text=f"CPC: ₹{metric['CPC']:.2f}", font=("Helvetica", 8), foreground="lightgreen").pack(anchor=W)
        ttkb.Label(metrics_frame, text=f"CTR: {metric['CTR']:.2f}%", font=("Helvetica", 8)).pack(anchor=W)
        ttkb.Label(metrics_frame, text=f"ROAS: {metric['ROAS']:.2f}x", font=("Helvetica", 8), foreground="lightgreen" if metric['ROAS'] > 1 else "lightcoral").pack(anchor=W)
        if metric["Note"]:
            ttkb.Label(metrics_frame, text=f"Note: {metric['Note']}", font=("Helvetica", 8), foreground="orange").pack(anchor=W)
        
        button_frame = ttkb.Frame(details_frame)
        button_frame.pack(anchor=W, pady=5)
        if media_paths:
            ttkb.Button(button_frame, text="Preview", style="info.TButton", 
                       command=lambda: MediaPreviewWindow(self.root, media_paths)).pack(side=LEFT, padx=2)
        ttkb.Button(button_frame, text="Open Ad", style="primary.TButton", 
                   command=lambda: webbrowser.open(ad["Ad Link"]) if ad["Ad Link"] else None).pack(side=LEFT, padx=2)

    def init_driver(self):
        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--headless")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.7031.114 Safari/537.36")
        try:
            driver = uc.Chrome(options=options, version_main=136)
            driver.set_page_load_timeout(30)
            self.log_message("Chrome driver initialized successfully.")
            return driver
        except Exception as e:
            self.log_message(f"Error initializing Chrome driver: {e}")
            raise

    def parse_active_time(self, active_time_text):
        if not active_time_text or active_time_text == "Unknown":
            self.log_message("Empty or unknown active time. Defaulting to 1 day.")
            return 1
        try:
            active_time_text = re.sub(r'http[s]?://\S+|www\.\S+', '', active_time_text).strip()
            if " - " in active_time_text:
                start_date_str, end_date_str = active_time_text.split(" - ")
                start_date = datetime.strptime(start_date_str.strip(), "%d %b %Y")
                end_date = datetime.strptime(end_date_str.split(" · ")[0].strip(), "%d %b %Y")
                days_active = (end_date - start_date).days
                if "Total active time" in active_time_text:
                    time_part = active_time_text.split("·")[1].replace("Total active time", "").strip()
                    if "hr" in time_part:
                        hours = int(time_part.split()[0])
                        days_active += hours / 24
                return max(days_active, 1)
            elif "Started running on" in active_time_text:
                parts = active_time_text.split("·")
                start_date = datetime.strptime(parts[0].replace("Started running on", "").strip(), "%d %b %Y")
                current_date = datetime.now()
                days_active = (current_date - start_date).days
                if len(parts) > 1:
                    time_part = parts[1].replace("Total active time", "").strip()
                    if "hr" in time_part:
                        hours = int(time_part.split()[0])
                        days_active += hours / 24
                return max(days_active, 1)
            else:
                self.log_message(f"Invalid active time format: '{active_time_text[:50]}...'. Defaulting to 1 day.")
                return 1
        except Exception as e:
            self.log_message(f"Error parsing active time '{active_time_text[:50]}...': {e}. Defaulting to 1 day.")
            return 1

    def extract_page_id(self, ad_link):
        if not ad_link:
            return "N/A"
        match = re.search(r'facebook\.com/(\d+)/', ad_link)
        if match:
            return match.group(1)
        match = re.search(r'facebook\.com/([^/?]+)', ad_link)
        if match:
            return match.group(1)
        return "N/A"

    def get_extension_from_content_type(self, content_type):
        mime_to_ext = {
            "image/jpeg": "jpg",
            "image/png": "png",
            "image/gif": "gif",
            "video/mp4": "mp4",
            "video/webm": "webm",
            "video/ogg": "ogv",
            "application/octet-stream": "bin"
        }
        content_type = content_type.lower()
        for mime, ext in mime_to_ext.items():
            if mime in content_type:
                return ext
        self.log_message(f"Unknown Content-Type '{content_type}'. Defaulting to 'bin'.")
        return "bin"

    def download_media(self, url, folder, filename_base, media_type="image"):
        try:
            if not os.path.exists(folder):
                os.makedirs(folder)
            response = requests.get(url, stream=True, timeout=10)
            if response.status_code != 200:
                self.log_message(f"Failed to download media from {url}: Status code {response.status_code}")
                return None
            content_type = response.headers.get("Content-Type", "application/octet-stream")
            ext = self.get_extension_from_content_type(content_type)
            filename_base = "".join(c for c in filename_base if c.isalnum() or c in (' ', '_', '-')).strip().replace(' ', '_')
            filename = f"{filename_base}.{ext}"
            filepath = os.path.join(folder, filename)
            counter = 1
            while os.path.exists(filepath):
                filename = f"{filename_base}_{counter}.{ext}"
                filepath = os.path.join(folder, filename)
                counter += 1
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            self.log_message(f"Saved media: {filepath} (Content-Type: {content_type})")
            return filepath
        except Exception as e:
            self.log_message(f"Error downloading media from {url}: {e}")
            return None

    def extract_ad_data(self, ad, index, advertiser_cache):
        try:
            WebDriverWait(ad, 5).until(EC.presence_of_element_located((By.XPATH, './/span')))
            advertiser_elems = ad.find_elements(By.XPATH, './/span[contains(@class, "x1lliihq") or contains(@class, "x193iq5w") or contains(@class, "page")] | .//a[contains(@href, "facebook.com") and not(contains(@href, "fbclid"))] | .//div[contains(@class, "advertiser")]')
            advertiser = "Unknown Advertiser"
            for elem in advertiser_elems:
                text = elem.text.strip()
                if text and len(text) > 2:
                    advertiser = text
                    break
            ad_link_elems = ad.find_elements(By.XPATH, './/a[contains(@href, "facebook.com") or contains(@href, "fbclid")]')
            ad_link = ad_link_elems[0].get_attribute('href') if ad_link_elems else ""
            page_id = self.extract_page_id(ad_link)
            if page_id != "N/A":
                advertiser = page_id
                advertiser_cache[page_id] = advertiser
            if advertiser == "Unknown Advertiser" and page_id in advertiser_cache:
                advertiser = advertiser_cache[page_id]
            ad_text_elems = ad.find_elements(By.XPATH, './/div[contains(@class, "body") or contains(@class, "text") or contains(@class, "content") or contains(@class, "_7jyr") or contains(@class, "description") or contains(@class, "ad-text")]')
            ad_text = ad_text_elems[0].text.replace("\n", " ").strip() if ad_text_elems else "..."
            active_time_elems = ad.find_elements(By.XPATH, './/span[contains(text(), "Started running on") or contains(text(), " - ")]')
            active_time = active_time_elems[0].text if active_time_elems else "Unknown"
            image_elems = ad.find_elements(By.XPATH, './/img[@src] | .//div[contains(@style, "background-image")]')
            image_urls = [img.get_attribute('src') for img in image_elems if img.get_attribute('src') and not img.get_attribute('src').endswith('.gif')]
            video_elems = ad.find_elements(By.XPATH, './/video[@src] | .//source[@src]')
            video_urls = [vid.get_attribute('src') for vid in video_elems if vid.get_attribute('src')]
            
            self.log_message(f"Parsed ad #{index}: {advertiser}")
            self.log_message(f"  Text: {ad_text[:50]}...")
            self.log_message(f"  Link: {ad_link}")
            self.log_message(f"  Page ID: {page_id}")
            self.log_message(f"  Active Time: {active_time} ({self.parse_active_time(active_time):.2f} days)")
            self.log_message(f"  Images: {len(image_urls)} found")
            self.log_message(f"  Videos: {len(video_urls)} found")
            
            return {
                "Advertiser": advertiser,
                "Ad Text": ad_text,
                "Ad Link": ad_link,
                "Active Time": active_time,
                "Image URLs": image_urls,
                "Video URLs": video_urls,
                "Page ID": page_id
            }
        except Exception as e:
            self.log_message(f"Error collecting raw ad data for ad #{index}: {e}")
            return {
                "Advertiser": "Unknown Advertiser",
                "Ad Text": "...",
                "Ad Link": "",
                "Active Time": "Unknown",
                "Image URLs": [],
                "Video URLs": [],
                "Page ID": "N/A",
                "Error": str(e),
                "Raw HTML": ad.get_attribute("outerHTML")[:500] if ad else "N/A"
            }

    def scrape_ads(self, keyword, country, num_scrolls):
        driver = None
        advertiser_cache = {}
        error_log = []
        try:
            driver = self.init_driver()
            search_url = (
                f"https://www.facebook.com/ads/library/?active_status=all&ad_type=all"
                f"&country={country}&q={keyword}&sort_data[direction]=desc&sort_data[mode]=relevancy_monthly_grouped&search_type=keyword_unordered"
            )
            self.log_message(f"Searching for ads with keyword: {keyword}")
            driver.get(search_url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            self.log_message("Page loaded successfully.")
            try:
                cookie_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Allow") or contains(text(), "Accept")]'))
                )
                cookie_btn.click()
                self.log_message("Accepted cookies.")
                time.sleep(1)
            except:
                self.log_message("No cookie popup detected.")
            last_ad_count = 0
            for i in range(num_scrolls):
                driver.execute_script("window.scrollBy(0, 1000);")
                self.log_message(f"Scrolling {i+1}/{num_scrolls}")
                time.sleep(2)
                ad_elements = driver.find_elements(By.XPATH, '//div[.//span[contains(text(), "Sponsored")]]')
                current_ad_count = len(ad_elements)
                self.log_message(f"Found {current_ad_count} ad(s) after scroll {i+1}")
                if current_ad_count == last_ad_count and i > 0:
                    self.log_message("No new ads loaded. Stopping scroll.")
                    break
                last_ad_count = current_ad_count
            with ThreadPoolExecutor(max_workers=8) as executor:
                raw_ads = list(filter(None, executor.map(lambda x: self.extract_ad_data(x[0], x[1], advertiser_cache), 
                                                        [(ad, i) for i, ad in enumerate(ad_elements, 1)])))
            ad_text_counts = Counter(ad["Ad Text"] for ad in raw_ads if ad["Ad Text"].strip() and ad["Ad Text"] != "...")
            ad_frequency = {ad_text: count for ad_text, count in ad_text_counts.items()}
            ads = []
            seen_ads = set()
            for index, ad_data in enumerate(raw_ads, start=1):
                try:
                    ad_text = ad_data["Ad Text"].strip().lower()
                    ad_link = ad_data["Ad Link"]
                    page_id = ad_data["Page ID"]
                    if not ad_text or ad_text == "..." or not ad_link or not (ad_data["Image URLs"] or ad_data["Video URLs"]):
                        self.log_message(f"Skipping ad #{index}: Incomplete data (Text: {ad_text[:50]}, Link: {ad_link}, Media: {len(ad_data['Image URLs'])} images, {len(ad_data['Video URLs'])} videos)")
                        error_log.append({
                            "Ad Index": index,
                            "Advertiser": ad_data["Advertiser"],
                            "Ad Text": ad_data["Ad Text"][:100],
                            "Ad Link": ad_link,
                            "Page ID": page_id,
                            "Active Time": ad_data["Active Time"][:100],
                            "Error": ad_data.get("Error", "Incomplete data"),
                            "Raw HTML": ad_data.get("Raw HTML", "N/A")
                        })
                        continue
                    ad_key = (page_id, ad_text, ad_link)
                    if ad_key in seen_ads:
                        continue
                    seen_ads.add(ad_key)
                    days_active = self.parse_active_time(ad_data["Active Time"])
                    variations = ad_frequency.get(ad_data["Ad Text"], 1) if ad_data["Ad Text"].strip() and ad_data["Ad Text"] != "..." else 1
                    ad_entry = {
                        "Advertiser": ad_data["Advertiser"],
                        "Ad Text": ad_data["Ad Text"],
                        "Ad Link": ad_link,
                        "Page ID": page_id,
                        "Active Time": ad_data["Active Time"],
                        "Days Active": days_active,
                        "Ad Variations": variations,
                        "Image URLs": ad_data["Image URLs"],
                        "Video URLs": ad_data["Video URLs"]
                    }
                    ads.append(ad_entry)
                except Exception as e:
                    self.log_message(f"Error parsing ad #{index}: {e}")
                    error_log.append({
                        "Ad Index": index,
                        "Advertiser": ad_data["Advertiser"],
                        "Ad Text": ad_data["Ad Text"][:100],
                        "Ad Link": ad_link,
                        "Page ID": page_id,
                        "Active Time": ad_data["Active Time"][:100],
                        "Error": str(e),
                        "Raw HTML": ad_data.get("Raw HTML", "N/A")
                    })
            if error_log:
                with open(self.debug_log_var.get(), "w", newline="", encoding="utf-8-sig") as f:
                    writer = csv.DictWriter(f, fieldnames=["Ad Index", "Advertiser", "Ad Text", "Ad Link", "Page ID", "Active Time", "Error", "Raw HTML"])
                    writer.writeheader()
                    writer.writerows(error_log)
                self.log_message(f"Saved {len(error_log)} problematic ads to {self.debug_log_var.get()}")
            return ads
        finally:
            if driver is not None:
                try:
                    time.sleep(1)
                    driver.quit()
                    self.log_message("Driver closed successfully.")
                except Exception as e:
                    self.log_message(f"Non-critical error during driver cleanup: {e}.")

    def save_to_csv(self, data, filename):
        if not data:
            self.log_message("No data to save.")
            return
        csv_data = [{k: v for k, v in ad.items() if k not in ["Image URLs", "Video URLs"]} for ad in data]
        for row in csv_data:
            for key, value in row.items():
                if isinstance(value, str):
                    row[key] = value.replace('\n', ' ').replace('\r', ' ')
        try:
            with open(filename, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=csv_data[0].keys(), quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                writer.writerows(csv_data)
            self.log_message(f"Saved {len(data)} ads to {filename}")
        except Exception as e:
            self.log_message(f"Error saving to CSV: {e}")

    def download_all_media(self, ad, ad_index):
        media_paths = []
        with ThreadPoolExecutor(max_workers=2) as executor:  # Reduced for stability
            futures = []
            for j, img_url in enumerate(ad["Image URLs"], 1):
                filename_base = f"ad_{ad_index}_{ad['Advertiser']}_image_{j}"
                futures.append(executor.submit(self.download_media, img_url, self.media_folder_var.get(), filename_base, "image"))
            for j, vid_url in enumerate(ad["Video URLs"], 1):
                filename_base = f"ad_{ad_index}_{ad['Advertiser']}_video_{j}"
                futures.append(executor.submit(self.download_media, vid_url, self.media_folder_var.get(), filename_base, "video"))
            for future in futures:
                result = future.result()
                if result:
                    media_paths.append(result)
        return media_paths

    def preprocess_text(self, text, advertiser=""):
        cache_key = (text, advertiser)
        if cache_key in translation_cache:
            return translation_cache[cache_key]
        if not text or text.strip() == "..." or len(text.strip()) < 10:
            result = f"Business consulting ad by {advertiser}" if advertiser and advertiser != "Unknown Advertiser" else "Generic business consulting ad"
        else:
            try:
                lang = detect(text)
                if lang != 'en':
                    translated = GoogleTranslator(source='auto', target='en').translate(text)
                    result = f"{translated.strip()} by {advertiser}" if advertiser and advertiser != "Unknown Advertiser" else translated.strip()
                else:
                    result = f"{text.strip()} by {advertiser}" if advertiser and advertiser != "Unknown Advertiser" else text.strip()
            except Exception as e:
                self.log_message(f"Translation error for text '{text[:50]}...': {e}")
                result = f"{text.strip()} by {advertiser}" if advertiser and advertiser != "Unknown Advertiser" else text.strip()
        translation_cache[cache_key] = result
        return result

    def predict_industry(self, ad_texts, advertisers):
        ad_texts = [self.preprocess_text(text, adv) for text, adv in zip(ad_texts, advertisers)]
        if classifier is None:
            return [{"Industry": "Software Development", "Confidence": 0.0, "Note": "Manual review needed - No classifier"}] * len(ad_texts)
        try:
            results = []
            batch_size = 8  # Adjust based on GPU memory
            for i in range(0, len(ad_texts), batch_size):
                batch_texts = ad_texts[i:i + batch_size]
                batch_results = classifier(batch_texts, candidate_labels=INDUSTRIES, multi_label=False)
                if isinstance(batch_results, dict):
                    batch_results = [batch_results]
                results.extend(batch_results)
            return [{
                "Industry": result["labels"][0],
                "Confidence": result["scores"][0],
                "Note": f"Low confidence ({result['scores'][0]:.2f}) - Manual review needed" if result["scores"][0] < CONFIDENCE_THRESHOLD else ""
            } for result in results]
        except Exception as e:
            self.log_message(f"Error classifying ad texts: {e}")
            return [{"Industry": "Software Development", "Confidence": 0.0, "Note": "Manual review needed - Classification error"}] * len(ad_texts)

    def estimate_metrics(self, ads):
        if not ads:
            self.log_message("No ads to estimate metrics for.")
            return []
        self.log_message(f"Estimating metrics for {len(ads)} unique ads")
        results = []
        low_confidence_ads = []
        seen_ads = set()
        
        ad_texts = [ad.get("Ad Text", f"Business consulting ad by {ad['Advertiser']}") for ad in ads]
        advertisers = [ad.get("Advertiser", "Unknown Advertiser") for ad in ads]
        industry_infos = self.predict_industry(ad_texts, advertisers)
        
        if len(industry_infos) != len(ads):
            self.log_message(f"Mismatch in industry_infos ({len(industry_infos)}) and ads ({len(ads)}). Using default industry.")
            industry_infos = [{"Industry": "Software Development", "Confidence": 0.0, "Note": "Manual review needed - Classification error"}] * len(ads)
        
        for i, ad in enumerate(ads):
            self.log_message(f"Processing ad {i+1}: Advertiser={ad.get('Advertiser', 'Unknown')}, Ad Text={ad.get('Ad Text', '...')[:50]}...")
            if ad.get("Advertiser") == "Unknown Advertiser":
                self.log_message(f"Skipping ad {i+1}: Invalid advertiser")
                continue
            ad_key = (ad.get("Advertiser"), ad.get("Ad Text", "...")[:100])
            if ad_key in seen_ads:
                self.log_message(f"Skipping ad {i+1}: Duplicate ad")
                continue
            seen_ads.add(ad_key)
            
            industry_info = industry_infos[i]
            industry = industry_info["Industry"]
            
            if industry in BENCHMARKS and industry != "Unclassified":
                cpc = BENCHMARKS[industry]["CPC"]
                ctr = BENCHMARKS[industry]["CTR"]
                conv_rate = BENCHMARKS[industry]["ConvRate"]
                aov = BENCHMARKS[industry]["AOV"]
            else:
                cpc = DEFAULT_CPC
                ctr = DEFAULT_CTR
                conv_rate = DEFAULT_CONVERSION_RATE
                aov = DEFAULT_AOV
                low_confidence_ads.append({
                    "Advertiser": ad.get("Advertiser"),
                    "Ad Text": ad.get("Ad Text", "..."),
                    "Confidence": industry_info["Confidence"],
                    "Note": industry_info["Note"]
                })
            
            days_active = ad.get("Days Active", DEFAULT_ACTIVE_DAYS)
            impressions = days_active * ad.get("Ad Variations", 1) * 400
            clicks = impressions * ctr
            spend = clicks * cpc
            conversions = clicks * conv_rate
            revenue = conversions * aov
            roas = revenue / spend if spend > 0 else 0
            
            result = {
                "Advertiser": ad.get("Advertiser"),
                "Industry": industry,
                "CPC": round(cpc, 2),
                "CTR": round(ctr * 100, 2),
                "Conversion Rate": round(conv_rate * 100, 2),
                "Estimated Spend (INR)": round(spend, 2),
                "Estimated Reach": round(impressions, 2),
                "ROAS": round(roas, 2),
                "Note": industry_info.get("Note", "")
            }
            results.append(result)
            self.log_message(f"Added result for ad {i+1}: Industry={industry}, Spend=₹{spend:.2f}")
        
        if low_confidence_ads:
            with open("low_confidence_ads.csv", "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=["Advertiser", "Ad Text", "Confidence", "Note"])
                writer.writeheader()
                writer.writerows(low_confidence_ads)
            self.log_message(f"Saved {len(low_confidence_ads)} low-confidence ads to low_confidence_ads.csv")
        
        self.log_message(f"Total valid estimates: {len(results)}")
        return results

    def save_metrics_to_csv(self, data):
        if not data:
            self.log_message("No data to save.")
            return
        fieldnames = ["Advertiser", "Industry", "CPC", "CTR", "Conversion Rate", 
                      "Estimated Spend (INR)", "Estimated Reach", "ROAS", "Note"]
        try:
            with open(self.metrics_output_var.get(), "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                writer.writerows(data)
            self.log_message(f"Saved {len(data)} estimates to {self.metrics_output_var.get()}")
        except Exception as e:
            self.log_message(f"Error saving to CSV: {e}")

    def show_top_5_ads(self, ads):
        if not ads:
            self.log_message("No ads to rank.")
            return []
        
        for ad in ads:
            days_active = ad["Days Active"] if ad["Days Active"] > 0 else DEFAULT_ACTIVE_DAYS
            ad["Hours Active"] = days_active * 24
        
        sorted_ads = sorted(ads, key=lambda x: x["Hours Active"], reverse=True)
        top_ads = []
        seen_keys = set()
        
        for ad in sorted_ads:
            ad_key = (ad["Ad Text"][:100], ad["Ad Link"], ad["Active Time"])
            if ad_key not in seen_keys and (ad["Image URLs"] or ad["Video URLs"]):
                top_ads.append(ad)
                seen_keys.add(ad_key)
            if len(top_ads) == 20:
                break
        
        if not top_ads:
            self.log_message("No valid ads with media to display after filtering.")
            return []
        
        self.ads_data = top_ads
        self.metrics_data = self.estimate_metrics(top_ads)
        self.media_files = []
        
        for widget in self.scrollable_frame.winfo_children():
            widget.destroy()
        
        for i, (ad, metric) in enumerate(zip(top_ads, self.metrics_data), 1):
            media_paths = self.download_all_media(ad, i)
            self.media_files.append(media_paths)
            self.create_ad_card(i, ad, metric, media_paths)
        
        self.log_message("Displayed top 5 ads in the right pane with ranking explanations.")
        return top_ads

    def start_scraping(self):
        if self.is_running:
            messagebox.showwarning("Warning", "Scraping is already in progress.")
            return
        
        try:
            num_scrolls = int(self.scrolls_var.get())
            if num_scrolls < 1:
                raise ValueError("Number of scrolls must be at least 1.")
        except ValueError:
            messagebox.showerror("Error", "Please enter a valid number of scrolls.")
            return
        
        self.is_running = True
        self.start_button.config(state=DISABLED)
        self.log_message("Starting scraping process...")
        
        def run_scraper():
            try:
                ads = self.scrape_ads(
                    self.keyword_var.get(),
                    self.country_var.get(),
                    num_scrolls
                )
                self.save_to_csv(ads, self.output_var.get())
                top_ads = self.show_top_5_ads(ads)
                self.log_message(f"Scraping completed. Found {len(ads)} ads, displayed top {len(top_ads)}.")
            except Exception as e:
                self.log_message(f"Scraping failed: {e}")
                messagebox.showerror("Error", f"Scraping failed: {e}")
            finally:
                self.is_running = False
                self.start_button.config(state=NORMAL)
                self.root.update()
        
        threading.Thread(target=run_scraper, daemon=True).start()

if __name__ == "__main__":
    root = ttkb.Window()
    
    style = ttkb.Style()
    style.configure("Card.TFrame", background="#2A2A2A", foreground="white", bordercolor="#3A3A3A")
    style.configure("CardHover.TFrame", background="#3A3A3A", foreground="white", bordercolor="#4A4A4A")
    style.configure("Media.TFrame", background="#1A1A1A", bordercolor="#3A3A3A", relief=RAISED, borderwidth=1)
    
    app = MetaAdsScraperApp(root)
    root.mainloop()