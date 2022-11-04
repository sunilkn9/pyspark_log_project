from cleanseddata import Starting
from curateddata import Started
from rawdataclean import Start

if __name__ == "__main__":
    # Start
    start = Start()
    starting = Starting()
    started = Started()
    start.read_from_s3()
    start.extract_columns()
    start.remove_character()
    starting.read_from_csv()
    starting.cleansed_data()
    started.data_from_s3_cleansed()



