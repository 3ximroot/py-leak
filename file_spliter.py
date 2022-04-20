import os,time,traceback

lines_per_file = 5000000
smallfile = None
splitted_path = '/home/nawaf/Canva_RF'
t = time.time()
for path, currentDirectory, files in os.walk("/home/nawaf/nawafmhm/Canva_RF/Data"):
    
    for file in files:
        if file.endswith(".txt"):
            print(files)
            folder_name = os.path.basename(path)
            file_name = os.path.splitext(file)[0]
            with open(os.path.join(path, file)) as bigfile:
                try:
                    for lineno, line in enumerate(bigfile):
                        if lineno % lines_per_file == 0:
                            if smallfile:
                                smallfile.close()
                            small_filename = file_name+'_{}.txt'.format(lineno + lines_per_file)
                            os.makedirs((splitted_path+'/splitters/'+folder_name+'/'), exist_ok=True)
                            n_path = 'splitters/'+folder_name+'/'+small_filename
                            print(os.path.join(splitted_path, n_path))
                            smallfile = open(os.path.join(splitted_path, n_path), "w")
                        smallfile.write(line)
                    if smallfile:
                        smallfile.close()
                except Exception as e:
                    print(traceback.format_exc())
print ("\n Time Taken: %.3f sec" % (time.time()-t))

