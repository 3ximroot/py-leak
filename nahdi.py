import os,time,traceback
from pymongo import MongoClient
 
 
myclient = MongoClient("mongodb://localhost:27017/")
 
# database
db = myclient["powned"]
 
collection = db["users"]

start_t = time.time()
TOTAL_FILES = 0
INSERTED_FILES = 0
INSERTED_ROWS = 0

def split_line(txt, source):
    try:
        if(txt != None):
            object_line = eval(txt)
            phone_object = next((x for x in object_line['custom_attributes'] if x['attribute_code'] == 'mobile_number'), None)
            phone = ''
            if phone_object != None:
                phone = phone_object['value']
            return {"firstname":object_line['firstname'],"lastname":object_line['lastname'],"phone":phone,
            "dob": object_line.get('dob', None),"email":object_line['email'],"adresses":object_line['addresses'], "source":source}
    except Exception:
        print(traceback.format_exc())

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
        with open(file_path,"r") as input_file:
            try:
                lines = input_file.read().splitlines()
                print("\n start file "+file_path+" =>" + str(P_ID))
                for line in lines:
                    if(line.startswith("{'id':")): 
                        if(line != None):
                            split = split_line(line,file_path)
                            if(split):
                                current_batch.append(split)
                                INSERTED_ROWS +=1
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
        

def path_splitter():
    global TOTAL_FILES
    reader_path = '/home/nawaf/nawafpr8e/nahdi'
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