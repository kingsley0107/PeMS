from Processors.PeMS_Incident_process import PeMS_Incident_Processor
import geopandas as gpd 
if __name__ == "__main__":
    year = 2018
    month = [1]
    district = 4
    path = rf"./incident_district{district}_{year}_{month}"
    t = PeMS_Incident_Processor(path,district=district,year= year,month=month)
    t.crawl_incident()
    t.merge_splited_incident_txt(t.file_save_path)
    t.csv2geojson(path)
    cliped = t.cliped_by_boundary(incident_path=t.file_save_path+'/incidents.geojson')

    Accidents = t.select_accident(cliped)
    Accidents_time_limited = t.set_time_limit(Accidents,15)
    Accidents_time_limited.to_file(rf'{path}/accidents_limited.geojson')