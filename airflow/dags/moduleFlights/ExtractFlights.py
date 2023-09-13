import os, json
import pandas as pd

class Extract_Flights:
    # init method or constructor
    def __init__(self, file):
        self.file = file

    def extract_data(self):
        if os.path.exists(self.file):
            # Opening JSON file
            f = open(self.file)
            # Load JSON into object
            data = json.load(f)
            # Close file
            f.close()
            
            # Extract Header Data
            id = data['id']
            unique_aircraft_type = data['fpl']['fpl_base'][0]['aircraft_type']
            unique_flight_rules = data['fpl']['fpl_base'][0]['flight_rules']
            #print("id: ", id, " unique_aircraft_type: ",unique_aircraft_type, " unique_aircraft_type: ", unique_flight_rules)

            #Extract Item Data
            df = pd.DataFrame(data['plots'])
            df_final = pd.DataFrame()
            try:
                df['plots_max_altitude'] = df['I062/380'].apply(lambda x: x['subitem7']['altitude'] if isinstance(x, dict) and 'subitem7' in x and 'altitude' in x['subitem7'] else None)
                df['plots_max_baro_vert_rate'] = df['I062/380'].apply(lambda x: x['subitem13']['baro_vert_rate'] if isinstance(x, dict) and 'subitem13' in x and 'baro_vert_rate' in x['subitem13'] else None)
                df['plots_max_mach'] = df['I062/380'].apply(lambda x: x['subitem27']['mach'] if isinstance(x, dict) and 'subitem27' in x and 'mach' in x['subitem27'] else None)
                df['plots_max_measured_flight_level'] = df['I062/136'].apply(lambda x: x['measured_flight_level'] if isinstance(x, dict) and 'measured_flight_level' in x else None)
                df_final = df[['plots_max_altitude', 'plots_max_baro_vert_rate', 'plots_max_mach', 'plots_max_measured_flight_level']]
            except Exception as e:
                for col in df.columns:
                    if 'altitude' in df[col].values:
                        df['plots_max_altitude'] = df[col].apply(lambda x: x['subitem7']['altitude'] if isinstance(x, dict) and 'subitem7' in x and 'altitude' in x['subitem7'] else None)
                    elif 'baro_vert_rate' in df[col].values:
                        df['plots_max_baro_vert_rate'] = df[col].apply(lambda x: x['subitem13']['baro_vert_rate'] if isinstance(x, dict) and 'subitem13' in x and 'baro_vert_rate' in x['subitem13'] else None)
                    elif 'mach' in df[col].values:
                        df['plots_max_mach'] = df[col].apply(lambda x: x['subitem27']['mach'] if isinstance(x, dict) and 'subitem27' in x and 'mach' in x['subitem27'] else None)
                    elif 'measured_flight_level' in df[col].values:
                        df['plots_max_measured_flight_level'] = df[col].apply(lambda x: x['measured_flight_level'] if isinstance(x, dict) and 'measured_flight_level' in x else None)
                    df_final = df[['plots_max_altitude', 'plots_max_baro_vert_rate', 'plots_max_mach', 'plots_max_measured_flight_level']]
            # print(df_final.shape)
                        
            total_row = df_final.shape[0]
            # print("total row", total_row)
            if(total_row == 0):
                raise Exception("no data found, please check manually if it has correct format") 
            else:
                # Get Min All Values
                df_min = df_final.min()
        
                # Get Max All Values
                df_max = df_final.max()
                result = {}
                result = df_max.to_dict()
        
                # Get Total Duration
                df['time_of_track'] = pd.to_datetime(df['time_of_track'], errors='coerce')
                total_duration = df['time_of_track'].max() - df['time_of_track'].min()
                hours = total_duration.seconds // 3600
                minutes = (total_duration.seconds % 3600) // 60
                seconds = total_duration.seconds % 60
                formatted_duration = f"{hours}h {minutes}m {seconds}s"
        
                # Append other data to dict
                result['id'] = id
                result['plots_duration'] = formatted_duration
        
                final_result = {
                    'id': result['id'],
                    'plots_duration': result['plots_duration'],
                    'plots_max_altitude': result['plots_max_altitude'],
                    'plots_max_baro_vert_rate': result['plots_max_baro_vert_rate'],
                    'plots_max_mach': result['plots_max_mach'],
                    'plots_max_measured_flight_level': result['plots_max_measured_flight_level'],
                    'unique_aircraft_type': unique_aircraft_type,
                    'unique_flight_rules': unique_flight_rules,
                }
                
                #return value
                return final_result


# folder_path = 'D:\\00 Project\\00 My Project\\Dataset\\Revalue_Nature\\Case 2\\' # Assuming this where all json file will be stored
# json_files = [pos_json for pos_json in os.listdir(folder_path) if pos_json.endswith('.json')]
# file_name = '100002.json'
# flight_obj = Extract_Flights(folder_path+file_name)
# flight = flight_obj.extract_data()
# print(flight['unique_aircraft_type'])