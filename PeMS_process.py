import zipfile
import gzip
import os
import requests
import time
from datetime import datetime

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class PeMs_Processor:
    def __init__(self, year=datetime.now().year) -> None:
        # pems提供的数据接口需要登陆访问
        self.year = year
        self.file_save_path = rf"./incident_{self.year}"
        self.base_url = "https://pems.dot.ca.gov/"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
            "Cookie": "nmstat=21419852-1054-bd10-3d60-aba16299e75e; __utmz=267661199.1684982596.2.2.utmcsr=blog.csdn.net|utmccn=(referral)|utmcmd=referral|utmcct=/w771792694/article/details/103075534; _ga_WLDEF7NZZ2=GS1.1.1685079639.3.1.1685080219.0.0.0; _ga_69TD0KNT0F=GS1.1.1685079640.3.1.1685080219.0.0.0; _ga=GA1.2.1557603639.1684395807; _gid=GA1.2.914196084.1685343138; _ga_FE9WWP5YXX=GS1.1.1685343135.1.0.1685343203.0.0.0; PHPSESSID=4e1da9c0d833279228d6d146756bb290; __utma=267661199.1557603639.1684395807.1685408584.1685423841.9; __utmc=267661199; __utmb=267661199.3.10.1685423841",
            "Host": "pems.dot.ca.gov",
        }
        self.session = requests.Session()
        self._session_configuration()
        self._params_configuration()

    def _session_configuration(self):
        # 保证每个实例维持完整会话
        retry_times = 5
        retry_delay = 1
        retry = Retry(total=retry_times, backoff_factor=retry_delay)
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _params_configuration(self):
        # 设置接口参数
        self.incident_params = {
            "srq": "clearinghouse",
            "district_id": "all",
            "geotag": "",
            "yy": self.year,
            "type": "chp_incidents_day",
            "returnformat": "text",
        }
        self.login_info = {
            "redirect": "",
            "username": "kingsleyl0107@gmail.com",
            "password": "Aa545591917.",
            "login": "Login",
        }

    def request_url(self, url, params=None):
        request_response = self.session.get(url, headers=self.headers, params=params)
        return request_response

    @staticmethod
    def incident_response_processor(incident_info_response):
        incident_data_list = incident_info_response.json()["data"]
        daily_file_urls = []
        for month, monthly_info in incident_data_list.items():
            for daily_info in monthly_info:
                daily_file_urls.append(daily_info["url"])
        return daily_file_urls

    def daily_incident_downloader(self, url_list):
        for today in url_list:
            incident_file_binary = self.session.get(
                self.base_url + today, headers=self.headers
            )
            os.makedirs(self.file_save_path, exist_ok=True)
            with open(rf"{self.file_save_path}/today.zip", "wb") as f:
                f.write(incident_file_binary.content)
            with zipfile.ZipFile(rf"{self.file_save_path}/today.zip", "r") as _zip_file:
                save_file = [
                    filename
                    for filename in _zip_file.namelist()
                    if "det" not in filename
                ][0]
                with _zip_file.open(save_file, "r") as gz_file:
                    with gzip.open(gz_file, "rb") as inner_file:
                        decompressed_data = inner_file.read()
                        # 将解压后的内容写入目标文件
                        with open(
                            rf"{self.file_save_path}/" + save_file[:-3], "wb"
                        ) as output_file:
                            output_file.write(decompressed_data)
            os.remove(rf"{self.file_save_path}/today.zip")
            # time.sleep(1)

    def crawl_incident(self):
        """_爬取PeMS特定年份的incident数据_

        Args:
            year (_type_): 声明incident目标年份数据
        """

        incident_info_response = self.request_url(self.base_url, self.incident_params)
        incident_url_list = self.incident_response_processor(incident_info_response)
        self.daily_incident_downloader(incident_url_list)


t = PeMs_Processor(2017)
t.crawl_incident()
