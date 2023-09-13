import os
import pprint
import pandas as pd
from moduleFlights.ExtractFlights import Extract_Flights

class Collect_Flights:
    
    # init method or constructor
    def __init__(self, folder_source, limit):
        self.folder_source = folder_source
        self.limit = limit

    def collect_data(self):
        folder_path = self.folder_source
        json_files = [pos_json for pos_json in os.listdir(folder_path) if pos_json.endswith('.json')]

        counter_file = 0
        max_file = self.limit
        dict_result = {"id": [], "plots_duration": [], "plots_max_altitude": [], "plots_max_baro_vert_rate": [], "plots_max_mach": [], "unique_aircraft_type": [], "unique_flight_rules": []}
        for file_name in json_files:
            if(counter_file < max_file):
                try:
                    flight_obj = Extract_Flights(folder_path+file_name)
                    flight = flight_obj.extract_data()
                    # dict_result[counter_file] = flight["id"]
                    dict_result["id"].append(flight["id"])
                    dict_result["plots_duration"].append(flight["plots_duration"])
                    dict_result["plots_max_altitude"].append(flight["plots_max_altitude"])
                    dict_result["plots_max_baro_vert_rate"].append(flight["plots_max_baro_vert_rate"])
                    dict_result["plots_max_mach"].append(flight["plots_max_mach"])
                    dict_result["unique_aircraft_type"].append(flight["unique_aircraft_type"])
                    dict_result["unique_flight_rules"].append(flight["unique_flight_rules"])
                    print("successfully extract file: ", str(folder_path+file_name))
                except Exception as e:
                    print("error reading file: ", str(folder_path+file_name), str(e))
                counter_file += 1
            elif(counter_file >= max_file):
                break;
        
        return dict_result

# collect_obj = Collect_Flights('D:\\00 Project\\00 My Project\\Dataset\\Revalue_Nature\\Case 2\\', 10)
# dict_result = collect_obj.collect_data()
# pp = pprint.PrettyPrinter(indent=2, width=30, compact=True)
# pp.pprint(dict_result)
# df = pd.DataFrame(dict_result)
# print(df.head(5))