import zipfile
import gzip
import os
import requests
import time
from datetime import datetime
import pandas as pd
import geopandas as gpd
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
        """通用request请求器

        Args:
            url (_type_): _待爬url_
            params (_type_, optional): _params_. Defaults to None.

        Returns:
            _type_: _description_
        """
        request_response = self.session.get(url, headers=self.headers, params=params)
        return request_response

    @staticmethod
    def incident_response_processor(incident_info_response):
        """特定的incident返回结果解析器

        Args:
            incident_info_response (_type_): _request返回的结果，包含incident的各项info_

        Returns:
            _type_: _返回特定年份中所有记录incident数据的url,以日为单位_
        """
        incident_data_list = incident_info_response.json()["data"]
        daily_file_urls = []
        for month, monthly_info in incident_data_list.items():
            for daily_info in monthly_info:
                daily_file_urls.append(daily_info["url"])
        return daily_file_urls

    def daily_incident_downloader(self, url_list):
        """下载写入incident文件

        Args:
            url_list (_type_): _处理返回的记录Incident的url列表，待下载_
        """
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
        print(
            rf"Successfully crawled {self.year} incident data. Path: {self.file_save_path}"
        )
        # time.sleep(1)

    def crawl_incident(self):
        """_爬取PeMS特定年份的incident数据_"""
        # 1.获取incident信息 2. 处理返回信息 3. 下载incident文件
        now = time.time()
        incident_info_response = self.request_url(self.base_url, self.incident_params)
        incident_url_list = self.incident_response_processor(incident_info_response)
        self.daily_incident_downloader(incident_url_list)
        print(f"total time: {time.time() - now}")

    @staticmethod
    def merge_splited_incident_txt(path):
        daily_report_list = []
        for _, _, report_names in os.walk(path):
            for daily_report_name in report_names:
                if daily_report_name.endswith(".txt"):
                    daily_report_list.append(daily_report_name)
        original_path = os.getcwd()
        os.chdir(path)
        total_incident_report = pd.DataFrame()
        for daily_report_file in daily_report_list:
            daily_file = pd.read_csv(
                daily_report_file,
                index_col=0,
                names=[
                    "CC Code",
                    "Incident Number",
                    "Timestamp",
                    "Description",
                    "Location",
                    "Area",
                    "Zoom Map",
                    "TB xy",
                    "Lat",
                    "Lon",
                    "District",
                    "County FIPS ID",
                    "City Fips ID",
                    "Freeway Number",
                    "Freeway Direction",
                    "State Postmile",
                    "Absolute Postmile",
                    "Severity",
                    "Duration/min",
                ],
            )
            daily_file["Timestamp"] = pd.to_datetime(daily_file["Timestamp"])
            daily_file = daily_file.set_index(["Timestamp"])
            total_incident_report = pd.concat([total_incident_report, daily_file])
            print(f"Merged {daily_report_file}")
        os.chdir(original_path)
        total_incident_report = total_incident_report.sort_index()
        total_incident_report.to_csv(path + "/" + "total_reports.csv", encoding="utf-8")
        print("Merged Successfully!")
        return total_incident_report

    @staticmethod
    def csv2geojson(path, columns=["Lon", "Lat"]):
        Lon, Lat = columns
        geo_csv = pd.read_csv(path + "/total_reports.csv")
        geo_csv["geometry"] = gpd.points_from_xy(geo_csv[Lon], geo_csv[Lat])
        geo_trans = gpd.GeoDataFrame(geo_csv, geometry="geometry", crs="epsg:4326")
        geo_trans.to_file(path + "/" + "incidents.geojson")
        return geo_trans


t = PeMs_Processor(2017)
# t.crawl_incident()
# t.merge_splited_incident_txt(t.file_save_path)
t.csv2geojson(r"./incident_2017")
