import utils
import os

#Downloads the raw data
utils.download_save_files()

#Preprocess the files and concatenates
raw_data_dir = os.path.join(os.getcwd(),"data","downloaded_files")
processed_data_dir = os.path.join(os.getcwd(),"data","silver_files")

if not os.path.isdir(processed_data_dir):
    processed_df = utils.processing_files(raw_data_dir)
    #Save processed data
    utils.save_silver(processed_df, processed_data_dir)
else:
    "The directory already exists"
