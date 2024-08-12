@echo off
start "" /wait "D:\iv_charts_4\data_analytics_backend\stop_iv_charts.bat"
timeout /t 15 /nobreak
start "" "D:\iv_charts_4\data_analytics_backend\kill_all.bat"