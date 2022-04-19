import os,time

start_t = time.time()
TOTAL_FILES = 0
INSERTED_FILES = 0
INSERTED_ROWS = 0

def split_line(txt):
    try:
        if(txt != None):
            object_line = eval(txt)
            phone_object = next((x for x in object_line['custom_attributes'] if x['attribute_code'] == 'mobile_number'), None)
            phone = ''
            if phone_object != None:
                phone = phone_object['value']
            return {"firstname":object_line['firstname'],"lastname":object_line['lastname'],"phone":phone,
            "dob": object_line['dob'],"email":object_line['email'],"adresses":object_line['addresses']}
    except:
        return txt



def inserter(pathes,P_ID):
    global TOTAL_FILES
    global INSERTED_FILES
    global INSERTED_ROWS
    for file_path in pathes:
        input_file = open(file_path,"r")
        current_batch = []
        insert_s_time = time.time()
        with open(file_path,"r") as input_file:
            lines = input_file.read().splitlines()
            print("\n start file "+file_path+" =>" + str(P_ID))
            for line in lines:
                    if(line.startswith("{'id':")): 
                        if(line != None):
                            split = split_line(line)
                            if(split):
                                current_batch.append(split)
                                INSERTED_ROWS +=1

            print(current_batch)
        

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