from os import listdir
from os.path import isfile, join




def read_file_system(folder):
    onlyfiles = [f for f in listdir(folder) if isfile(join(folder, f))]
    print(onlyfiles)


folder=input("Write the folder the files are saved in: ")
read_file_system(folder)