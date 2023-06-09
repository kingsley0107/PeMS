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
from configs.cookiesPool import cookies

ACCIDENT_CODES = ['1179','1181','1182','1183','20001','20002']

MONTH_NUM_MAP = {
    1:'January',
    2:'February',
    3:'March',
    4:'April',
    5:'May',
    6:'June',
    7:'July',
    8:'August',
    9:'September',
    10:'October',
    11:'November',
    12:'December'
}

class PeMS_Incident_Processor:
    def __init__(
        self,
        path,
        district,
        year=datetime.now().year,
        month=None
    ) -> None:
        # pems提供的数据接口需要登陆访问
        self.year = year
        if isinstance(month,int):
            self.month=MONTH_NUM_MAP[month]
        elif isinstance(month,list):
            self.month = list(map(lambda x: MONTH_NUM_MAP[x],month))
        elif not month:
            pass
        else:
            raise Exception("type error")
        self.district=district
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
    def response_processor(incident_info_response,target_month=None):
        """特定的incident返回结果解析器

        Args:
            incident_info_response (_type_): _request返回的结果，包含incident的各项info_

        Returns:
            _type_: _返回特定年份中所有记录incident数据的url,以日为单位_
        """
        incident_data_list = incident_info_response.json()["data"]
        if isinstance(target_month,int):
            incident_data_list = {target_month:incident_data_list[target_month]}
        elif isinstance(target_month,list):
            incident_data_list = {month:incident_data_list[month] for month in target_month}
        elif not target_month:
            pass
        daily_file_urls = []
        for month, monthly_info in incident_data_list.items():
            for daily_info in monthly_info:
                daily_file_urls.append(daily_info["url"])
        return daily_file_urls

    def daily_incident_downloader(self, url_list, filter_word="det"):
        """下载写入incident文件

        Args:
            url_list (_type_): _处理返回的记录Incident的url列表，待下载_
        """
        for today in url_list:
            incident_file_binary = self.session.get(
                self.base_url + today, headers=self.headers
            )
            os.makedirs(self.file_save_path, exist_ok=True)
            try:
                with open(rf"{self.file_save_path}/today.zip", "wb") as f:
                    f.write(incident_file_binary.content)
                with zipfile.ZipFile(rf"{self.file_save_path}/today.zip", "r") as _zip_file:
                    save_file = [
                        filename
                        for filename in _zip_file.namelist()
                        if filter_word not in filename
                    ][0]
                    with _zip_file.open(save_file, "r") as gz_file:
                        with gzip.open(gz_file, "rb") as inner_file:
                            decompressed_data = inner_file.read()
                            # 将解压后的内容写入目标文件
                            with open(
                                rf"{self.file_save_path}/" + save_file[:-3], "wb"
                            ) as output_file:
                                output_file.write(decompressed_data)
            except:
                pass
            os.remove(rf"{self.file_save_path}/today.zip")
            print(f"Downloading {today}")
        print(
            rf"Successfully crawled {self.year} incident data. Path: {self.file_save_path}"
        )
        # time.sleep(1)

    def crawl_incident(self):
        """_爬取PeMS特定年份的incident数据_"""
        # 1.获取incident信息 2. 处理返回信息 3. 下载incident文件
        now = time.time()
        incident_info_response = self.request_url(self.base_url, self.incident_params)
        incident_url_list = self.response_processor(incident_info_response,self.month)
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
        print("transfering into geo data...")
        Lon, Lat = columns
        geo_csv = pd.read_csv(path + "/total_reports.csv")
        geo_csv = geo_csv.dropna(subset=['Lon','Lat'])
        geo_csv["geometry"] = gpd.points_from_xy(geo_csv[Lon], geo_csv[Lat])
        geo_trans = gpd.GeoDataFrame(geo_csv, geometry="geometry", crs="epsg:4326")
        geo_trans = geo_trans[geo_trans.geom_type == 'Point']
        geo_trans.to_file(path + "/" + "incidents.geojson")
        print("finished transfering!")
        return geo_trans

    def cliped_by_boundary(self,incident_path):
        incident_points = gpd.read_file(incident_path)
        points_in_bounds = incident_points[incident_points['District']==self.district]
        return points_in_bounds.to_crs("epsg:4326")
    
    @staticmethod
    def select_accident(gdf):
        return gdf[gdf['Description'].str.startswith(tuple(ACCIDENT_CODES))]
    
    @staticmethod
    def set_time_limit(gdf,threshold):
        gdf['Duration/min'] = gdf['Duration/min'].fillna('0').astype('int')
        gdf = gdf[gdf['Duration/min']>=threshold]
        return gdf.reset_index(drop=True)


