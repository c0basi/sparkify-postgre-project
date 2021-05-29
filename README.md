# sparkify-postgre-project
Sparkify dataset to analyse customer and artist actions. The log_file contains data about which songs customers listen to and the song_file contains data about songs and artist. The data model is done using postgres with python

Steps to run the code:
1 Make sure you have at leats python 2.7 installed, and pyscopg2
2 run the create_tables.py first to connect to the database and create the tables 
3 run the etl.py to input the correct data into the tables
4 test.ipynb and tel.ipynb are intermediate files to test the process of creating the dimensional model
