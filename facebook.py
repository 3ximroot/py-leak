from distutils.log import error
import os,time,re,traceback
from pymongo import MongoClient
 
 
myclient = MongoClient("mongodb://localhost:27017/")
 
# database
db = myclient["powned"]
 
collection = db["facebook"]

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
        input_file = open(file_path,"r")
        current_batch = []
        insert_s_time = time.time()
        with open(file_path,"r", encoding='utf-8', errors='ignore') as input_file:
            try:
                print("\n start file "+file_path+" =>" + str(P_ID))
                results = [[r.strip().replace('"','').replace('\'\'','') for r in line.split(',')] for line in input_file.read().splitlines()]
                #results = [[r for r in row if r] for row in results if row]
                #print(results)
                lines =  [row for row in results if row]
                line_no = 1
                #print(lines)
                #id0,phone1,first_name2,last_name3,email2,birthday5,gender4,locale,hometown 12,location13,link5,username7

                #id0,phone3,first_name6,last_name7,email2,birthday5,gender8,locale,hometown 16,location17,link9,username11
                #id,phone,first_name,last_name,email,birthday,gender,locale,hometown,location,link
                for line in lines: 
                    if(len(line) >1):
                        current_batch.append({
                        "fid":line[0] if 0 < len(line) else None,
                        #"email":line[2] if 2 < len(line) else None,
                        "phone":line[1] if 1 < len(line) else None,
                        "username":line[7] if 7 < len(line) else None,
                        "first_name":line[2] if 2 < len(line) else None,
                        "last_name":line[3] if 3 < len(line) else None,
                        "link":line[5] if 5 < len(line) else None,
                        #"birthday":line[5] if 5 < len(line) else None,
                        "gender":line[4] if 4 < len(line) else None,
                        "city":line[12] if 12 < len(line) else None,
                        "location":line[13] if 13 < len(line) else None,
                        "source":file_path,
                        "line":line_no
                        })
                        INSERTED_ROWS +=1
                    line_no +=1
            except Exception:
                print(traceback.format_exc())
        INSERTED_FILES +=1
        if(len(current_batch) >0):
            try:
                collection.insert_many(current_batch, ordered=False)
                delete_inserted_file(file_path)
                print("\n inserted "+str(len(lines))+" in " + str(time.time()-insert_s_time)+" =>" + str(P_ID))
                print("\n FILES PROGRESS "+str(INSERTED_FILES)+"/"+str(TOTAL_FILES)+" =>" + str(P_ID))
                print("\n ROWS INSERTED "+str(INSERTED_ROWS))
            except Exception:
                print(traceback.format_exc())
                print('** File'+file_path+' failed to insert => skip')
        

def path_splitter():
    global TOTAL_FILES
    reader_path = '/home/nawaf/facebook/facebook'
    pathes = []
    for path, currentDirectory, files in os.walk(reader_path):
        for file in files:
            if file.endswith(".txt"):
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
