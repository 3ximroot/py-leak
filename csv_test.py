
import os,time,traceback
from csv import reader


start_t = time.time()
TOTAL_FILES = 0
INSERTED_FILES = 0
INSERTED_ROWS = 0

def split_line(txt):
    delims = ['	']
    for d in delims:
        result = txt.strip().split(d)
        if len(result) >=2: 
            return result

    return [txt] # If nothing worked, return the input

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
        lines = 0
        with open(file_path,"r") as input_file:
            try:
                print("\n start file "+file_path+" =>" + str(P_ID))
                readerer = reader(input_file)
                for i, line in enumerate(readerer): 
                    try:
                        spliter = line
                        if(len(spliter) >=2):
                            current_batch.append({"fname":spliter[0], "lname":spliter[1],"address":spliter[2],"city":spliter[3],"state":spliter[4],"zip":spliter[5],"phone":spliter[6],"email":spliter[7],"phone_carrier":spliter[8],"gender":spliter[9], "source":file_path})
                            INSERTED_ROWS +=1
                            lines +=1
                        else:
                            print(spliter)
                    except Exception:
                        print(traceback.format_exc())
                        print(line)
                print(current_batch[1:100])
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
    #batches = np.array_split(pathes,producers_count)
    return pathes

def main():
    pathes = path_splitter()
    inserter(pathes,1)
    print ("\n Time Taken: %.3f sec" % (time.time()-start_t))


if __name__ == '__main__':
    main()
