import gzip
import os
import io
import requests
import time
from datetime import datetime
from PeMS_Incident_process import PeMS_Incident_Processor
from configs.cookiesPool import cookies
import re


class PeMS_Flow_Processor(PeMS_Incident_Processor):
    def __init__(
        self,
        path,
        district_id=3,
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
        self._5_min_flow_params = {
            "srq": "clearinghouse",
            "district_id": self.district_id,
            "geotag": "",
            "yy": self.year,
            "type": "station_5min",
            "returnformat": "text",
        }
        self._login_info = {
            "redirect": "",
            "username": "kingsleyl0107@gmail.com",
            "password": "Aa545591917.",
            "login": "Login",
        }

    def daily_flow_downloader(self, url_list):
        """下载写入incident文件

        Args:
            url_list (_type_): _处理返回的记录Incident的url列表，待下载_
        """
        for today in url_list:
            flow_file_binary = self.session.get(
                self.base_url + today, headers=self.headers
            )
            Content_Disposition = flow_file_binary.headers.get("Content-Disposition")
            file_name = re.findall(
                r"filename=(.+)", flow_file_binary.headers.get("Content-Disposition")
            )[0][:-3]
            os.makedirs(self.file_save_path, exist_ok=True)
            # 创建BytesIO对象并写入压缩数据
            compressed_data = io.BytesIO(flow_file_binary.content)
            # 解压缩数据
            with gzip.GzipFile(fileobj=compressed_data, mode="rb") as gzip_file:
                # 读取解压缩后的数据
                decompressed_data = gzip_file.read()
                with open(rf"{self.file_save_path}/" + file_name, "wb") as output_file:
                    output_file.write(decompressed_data)
            print(f"download {file_name}")
        print(
            rf"Successfully crawled {self.year} flow data. Path: {self.file_save_path}"
        )

    def crawl_flow(self):
        """_爬取PeMS特定年份的5分钟flow流量数据_"""
        # 1.获取incident信息 2. 处理返回信息 3. 下载incident文件
        now = time.time()
        incident_info_response = self.request_url(
            self.base_url, self._5_min_flow_params
        )
        incident_url_list = self.response_processor(incident_info_response)
        self.daily_flow_downloader(incident_url_list)
        print(f"total time: {time.time() - now}")


if __name__ == "__main__":
    t = PeMS_Flow_Processor(r"./flow_2017", 4, 2017)
    t.crawl_flow()
