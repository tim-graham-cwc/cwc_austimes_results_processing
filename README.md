# AusTIMES Results Processing
## Info
- This project is used by Climateworks Centre to process results from the AusTIMES model

## Setup
- Clone this repository on your machine
- Once setup, you can install all required packages by running the following command in the project directory:
> pip install -r requirements.txt

## Running the scripts
- To run processing you must ensure input files are added to the /inputs directory. Required files are detailed in /inputs/info.txt file
- The output directory can be changed within the directories.py file
- Run options are found near the top of the processing.py file
- If you wish to output excel visualisation files, you must be connected to the Monash VPN, as the templates are stored on 
the S: Drive. They are stored here and not on Github  as they should not be made publicly available.

## Development
- Ensure that changes are made on branches and not to the master branch
- All updates should be reviewed before being merged to the master branch
- When commiting to git, DO NOT upload results files to the repository, as this will make them publicly available

## Connection to SQL Server
- You may need to update the server credentials stored in the sql-server-details,py file in order to connect to the SQL database on the machine from which you are running these scripts