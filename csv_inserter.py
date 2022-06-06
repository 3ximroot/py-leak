import os,time,traceback,csv
from pymongo import MongoClient
import gc
 
 
myclient = MongoClient("mongodb://localhost:27017/")
 
# database
db = myclient["powned"]
 
collection = db["users2"]

start_t = time.time()
TOTAL_FILES = 0
INSERTED_FILES = 0
INSERTED_ROWS = 0


def delete_inserted_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
    return True


def inserter(pathes,P_ID):
    global TOTAL_FILES
    global INSERTED_FILES
    global INSERTED_ROWS
    for file_path in pathes:
        gc.collect()
        input_file = open(file_path,"r")
        current_batch = []
        insert_s_time = time.time()
        lines = 0
        with open(file_path,"r") as input_file:
            try:
                print("\n start file "+file_path+" =>" + str(P_ID))
                reader = csv.reader(input_file)
                for i, line in enumerate(reader): 
                    try:
                        spliter = line
                        if(len(spliter) >=2):
                            #"FIRST","LAST","ADDRESS","CITY","STATE","ZIP","CELLPHONE","EMAIL","PHONE CARRIER","GENDER","HOME_VALUE","HOUSEHOLD_INCOME",,,,,,,,,,,,,,,,,,,,,
                            current_batch.append({"fname":spliter[0], "lname":spliter[1],"address":spliter[2],"city":spliter[3],"state":spliter[4],"zip":spliter[5],"phone":spliter[6],"email":spliter[7],"phone_carrier":spliter[8],"gender":spliter[9], "source":file_path})
                            INSERTED_ROWS +=1
                            lines +=1
                        else:
                            print(spliter)
                    except Exception:
                        print(traceback.format_exc())
                        print(line)
            except:
                print(traceback.format_exc())
                print('** File'+file_path+' failed to insert => skip')
        INSERTED_FILES +=1
        if(len(current_batch) >0):
            try:
                collection.insert_many(current_batch, ordered=False)
                delete_inserted_file(file_path)
                print("\n inserted  in " + str(time.time()-insert_s_time)+" =>" + str(P_ID))
                print("\n FILES PROGRESS "+str(INSERTED_FILES)+"/"+str(TOTAL_FILES)+" =>" + str(P_ID))
                print("\n ROWS INSERTED "+str(INSERTED_ROWS))
            except:
                print(traceback.format_exc())
                print('** File'+file_path+' failed to insert => skip')
        

def path_splitter():
    global TOTAL_FILES
    reader_path = '/home/nawaf/dbx'
    pathes = []
    for path, currentDirectory, files in os.walk(reader_path):
        for file in files:
            if file.endswith(".csv"):
                TOTAL_FILES +=1
                start_t = time.time()
                pathes.append(os.path.join(path, file))
    return pathes

def main():
    pathes = path_splitter()
    inserter(pathes,1)
    print ("\n Time Taken: %.3f sec" % (time.time()-start_t))


if __name__ == '__main__':
    main()
