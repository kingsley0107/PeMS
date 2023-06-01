import gzip
import os
import io
import requests
import time
from datetime import datetime
import pandas as pd
import geopandas as gpd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from PeMS_flow_process import PeMS_Flow_Processor
from cookiesPool import cookies
import re


class PeMS_Station_Meta(PeMS_Flow_Processor):
    def __init__(
        self,
        path,
        district_id=4,
        year=datetime.now().year,
    ) -> None:
        # pems提供的数据接口需要登陆访问
        self.district_id = district_id
        self.year = year
        self.file_save_path = path
        self.base_url = "https://pems.dot.ca.gov/"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
            "Cookie": cookies,
            "Host": "pems.dot.ca.gov",
        }
        self.session = requests.Session()
        self._session_configuration()
        self._params_configuration()

    def _params_configuration(self):
        # 设置接口参数
        self._station_meta_params = {
            "srq": "clearinghouse",
            "district_id": self.district_id,
            "geotag": "null",
            "yy": self.year,
            "type": "meta",
            "returnformat": "text",
        }
        self._login_info = {
            "redirect": "",
            "username": "kingsleyl0107@gmail.com",
            "password": "Aa545591917.",
            "login": "Login",
        }

    def Station_Meta_downloader(self, url_list):
        """下载写入incident文件

        Args:
            url_list (_type_): _处理返回的记录Incident的url列表，待下载_
        """
        os.makedirs(self.file_save_path, exist_ok=True)
        for today in url_list:
            flow_file_binary = self.session.get(
                self.base_url + today, headers=self.headers
            )
            Content_Disposition = flow_file_binary.headers.get("Content-Disposition")
            file_name = re.findall(r"filename=(.+)", Content_Disposition)[0]
            with open(self.file_save_path + "/" + file_name, "wb") as f:
                f.write(flow_file_binary.content)
            print(f"download {file_name}")
        print(
            rf"Successfully crawled {self.year} flow data. Path: {self.file_save_path}"
        )

    def crawl_station_meta(self):
        """_爬取PeMS特定年份的station_meta_"""

        now = time.time()
        incident_info_response = self.request_url(
            self.base_url, self._station_meta_params
        )
        incident_url_list = self.response_processor(incident_info_response)
        self.Station_Meta_downloader(incident_url_list)
        print(f"total time: {time.time() - now}")

    @staticmethod
    def Station_Meta_to_csv(folder_path):
        full_txt = pd.DataFrame()
        ori_path = os.getcwd()
        os.chdir(folder_path)
        _, _, text_meta = list(os.walk("."))[0]
        for _text_file in text_meta:
            meta_df = pd.read_csv(f"{_text_file}", delimiter="\t")
            full_txt = pd.concat([full_txt, meta_df])
            full_txt = full_txt.drop_duplicates(["ID"]).reset_index(drop=True)
        full_txt.to_csv(r"./total_stations.csv")
        os.chdir(ori_path)
        return full_txt

    @staticmethod
    def csv2geojson(path, columns=["Longitude", "Latitude"]):
        Lon, Lat = columns
        geo_csv = pd.read_csv(path + "/total_stations.csv")
        geo_csv["geometry"] = gpd.points_from_xy(geo_csv[Lon], geo_csv[Lat])
        geo_trans = gpd.GeoDataFrame(geo_csv, geometry="geometry", crs="epsg:4326")
        geo_trans.to_file(path + "/" + "stations.geojson")
        return geo_trans


t = PeMS_Station_Meta(r"./station_2017", 4, 2017)
# t.crawl_station_meta()
t.Station_Meta_to_csv(r"./station_2017")
t.csv2geojson(r"./station_2017")
