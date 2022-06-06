# Read and store content
# of an excel file 
read_file = pd.read_excel ("Test.xlsx")
  
# Write the dataframe object
# into csv file
read_file.to_csv ("Test.csv", 
                  index = None,
                  header=True)
    