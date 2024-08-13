Here are the steps to set up the chart backend- 
1. change the path of quantlib's wheel file in requirements.txt
2. make environment and install the modules as listed in requirements.txt
3. change the db_config file to have the details of the db of your system or the system where you have the db
db_name = YOUR DB NAME(change)
pg_user = 'postgres'
pg_pass = YOUR DB PASSWORD(change)
pg_host = IP OF THE SYSTEM WHERE DB IS HOSTED(change)
4. make sure that the app.py and app_old.py are running on 8811 and 8801 respectively, if not then port has to be changed have to be made in the frontend also.
5. if you have different api keys for XTS, update it in xts_connect.py(now using BR052)

Here are the steps to set up the chart frontend- 
1. open the code in vs code and install node (npm i), if node is not installed in the system then first install node from https://nodejs.org/en/download/prebuilt-installer
2. make environment file (.env), copy the code from .env.example
VITE_AUTH_API_BASE_URL=SYSTEM_IP:APP_PORT(change)
VITE_CHART_BASE_URL=SYSTEM_IP:APP_PORT(change)
VITE_CHART_OLD_BASE_URL=SYSTEM_IP:APP_OLD_PORT(change)
3. change the port to the desired port on which the frontend should run (search --port)
