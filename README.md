# tap-awin

singer.io tap- Extracts data from the Awin REST API, written in python 3.5.

Author: Ashwani Singh (ashwani.s@blueoceanmi.com)


1. Install

    >python setup.py install 
    >tap-awin -h

2. Execution and configuration options:

    tap-awin takes two inputs arguments
     
     I. --config:  It takes a configuration file as authentication parameters and parameters are:
        1) "accessToken": Awin provided access token.
        2) "start_date" : This parameter will be used to sync data when tap will be executed for the first time or if --state ption is not 		   passed.
	3) "user_agent" : To identify user in network.
        4) "increament" : Incremental load options in days.
	5) "transactions", "aggregatedReport", "aggregatedByCreative" and "programmes": additional filter options to setup filters for data extraction, startDate and endDate will be taken from "start_date" parameter from config file, for more about data filter refer http://wiki.awin.com/index.php/Publisher_API and http://wiki.awin.com/index.php/Advertiser_API .

This tap supports incremental data load any error in data load, user need to truncate any data loaded in same execution and re-run once again, to identify data, start_date and end_date columns has been added into each dataset.

Note: This Tap has been validated for Advertiser data only, due no data for publisher for validation is not done. 

    
3. Running the application:
    > tap-awin  --config config.json  [--state  state.json]

