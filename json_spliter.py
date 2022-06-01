import json
import os,time,traceback

lines_per_file = 5000000
smallfile = None
splitted_path = '/home/nawaf/Canva_RF'
t = time.time()
for path, currentDirectory, files in os.walk("/home/nawaf/nawafmhm/Canva_RF/Data"):
    
    for file in files:
        if file.endswith(".json"):
            print(files)
            folder_name = os.path.basename(path)
            file_name = os.path.splitext(file)[0]

            with open(file,'r') as in_json_file:

                # Read the file and convert it to a dictionary
                json_obj_list = json.load(in_json_file)

                for json_obj in json_obj_list:
                    small_filename = file_name+'_{}.json'.format(lineno + lines_per_file)
                    os.makedirs((splitted_path+'/splitters/'+folder_name+'/'), exist_ok=True)
                    n_path = 'splitters/'+folder_name+'/'+small_filename
                    filename=json_obj['_id']+'.json'

                    with open(filename, 'w') as out_json_file:
                        # Save each obj to their respective filepath
                        # with pretty formatting thanks to `indent=4`
                        json.dump(json_obj, out_json_file, indent=4)