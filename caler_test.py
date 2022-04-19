import os,time,re
start_t = time.time()
TOTAL_FILES = 0
INSERTED_FILES = 0
INSERTED_ROWS = 0

def split_line(txt):
    striper = txt.strip().split(' ',maxsplit=3)
    row =  [eval(striper[3])[0]['name'],striper[2]]
    return row


def inserter(pathes,P_ID):
    for file_path in pathes:
        input_file = open(file_path,"r")
        current_batch = []
        insert_s_time = time.time()
        with open(file_path,"r") as input_file:
            lines = input_file.read().splitlines()
            print("\n start file "+file_path+" =>" + str(P_ID))
            for line in lines: 
                split = split_line(line)
                print(split)

def path_splitter(producers_count):
    global TOTAL_FILES
    reader_path = '/home/nawaf/nawafpr8e/caller_id'
    pathes = []
    9666
    for path, currentDirectory, files in os.walk(reader_path):
        for file in files:
            if file.endswith(".txt"):
                TOTAL_FILES +=1
                start_t = time.time()
                pathes.append(os.path.join(path, file))
    return pathes

def main():
    pathes = path_splitter(1)
    inserter(pathes,1)
    print ("\n Time Taken: %.3f sec" % (time.time()-start_t))


if __name__ == '__main__':
    main()
