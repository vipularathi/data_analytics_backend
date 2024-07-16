# import logging
# import os
# import sys
# from datetime import datetime
# from logging.handlers import TimedRotatingFileHandler
#
# import pandas as pd
# import pytz
# from dateutil.relativedelta import relativedelta
#
# # root_dir = os.path.dirname(os.path.abspath(__file__))
# # logs_dir = os.path.join(root_dir, 'logs/')
# # data_dir = os.path.join(root_dir, 'entity_data/')
# # dirs = [logs_dir, data_dir]
# # # dirs = [logs_dir, provider_dir]
# # status = [os.makedirs(_dir, exist_ok=True) for _dir in dirs if not os.path.exists(_dir)]
#
# root_dir = os.path.dirname(os.path.abspath(__file__))
# logs_dir = os.path.join(root_dir, 'logs/')
# data_dir = os.path.join(root_dir,'data/')
# dir_list = [logs_dir, data_dir]
# status = [os.makedirs(_dir, exist_ok=True) for _dir in dir_list if not os.path.exists(_dir)]
# instruments_path = os.path.join(data_dir, 'instruments.csv')  # Unique for each day. No History available.
#
# holidays_23 = ['2023-01-26', '2023-03-07', '2023-03-30', '2023-04-04', '2023-04-07', '2023-04-14', '2023-05-01', '2023-06-29', '2023-08-15', '2023-09-19', '2023-10-02', '2023-10-24', '2023-11-14', '2023-11-27', '2023-12-25']
# holidays_24 = ['2024-01-22', '2024-01-26', '2024-03-08', '2024-03-25', '2024-03-29', '2024-04-11', '2024-04-17', '2024-05-01','2024-05-20', '2024-06-17', '2024-07-17', '2024-08-15', '2024-10-02', '2024-11-01', '2024-11-15', '2024-12-25']
# holidays_24.append('2024-03-20') #add unusual holidays
# holidays = holidays_23 + holidays_24
# b_days = pd.bdate_range(start=datetime.now()-relativedelta(months=3), end=datetime.now(), freq='C', weekmask='1111100',
#                         holidays=holidays)
# b_days = b_days.append(pd.DatetimeIndex([pd.Timestamp(year=2024, month=1, day=20), pd.Timestamp(year=2024, month=3, day=2),
#                                           pd.Timestamp(year=2024, month=5, day=18)])) #removed contigency trading drill dates from unusual business days
#
# b_days = b_days[b_days <= datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)].drop_duplicates().sort_values()
# today, yesterday = b_days[-1], b_days[-2]
# IST = pytz.timezone('Asia/Kolkata')
#
#
# def define_logger():
#     # Logging Definitions
#     log_lvl = logging.DEBUG
#     console_log_lvl = logging.INFO
#     _logger = logging.getLogger('arathi')
#     # logger.setLevel(log_lvl)
#     _logger.setLevel(console_log_lvl)
#     log_file = os.path.join(logs_dir, f'logs_arathi_{datetime.now().strftime("%Y%m%d")}.log')
#     handler = TimedRotatingFileHandler(log_file, when='D', delay=True)
#     handler.setLevel(log_lvl)
#     console = logging.StreamHandler(stream=sys.stdout)
#     console.setLevel(console_log_lvl)
#     # formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')  #NOSONAR
#     # formatter = logging.Formatter('%(asctime)s %(levelname)s %(filename)s %(funcName)s %(message)s')
#     formatter = logging.Formatter('%(asctime)s %(levelname)s <%(funcName)s> %(message)s')
#     handler.setFormatter(formatter)
#     console.setFormatter(formatter)
#     _logger.addHandler(handler)  # Comment to disable file logs
#     _logger.addHandler(console)
#     # logger.propagate = False  # Removes AWS Level Logging as it tracks root propagation as well
#     return _logger
#
#
# logger = define_logger()

import logging
import os
import sys
from datetime import datetime, time, timedelta
from logging.handlers import TimedRotatingFileHandler

import pandas as pd
import pytz
from dateutil.relativedelta import relativedelta

# root_dir = os.path.dirname(os.path.abspath(__file__))
# logs_dir = os.path.join(root_dir, 'logs/')
# data_dir = os.path.join(root_dir, 'entity_data/')
# dirs = [logs_dir, data_dir]
# # dirs = [logs_dir, provider_dir]
# status = [os.makedirs(_dir, exist_ok=True) for _dir in dirs if not os.path.exists(_dir)]

root_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = os.path.join(root_dir, 'logs/')
data_dir = os.path.join(root_dir,'data/')
dir_list = [logs_dir, data_dir]
status = [os.makedirs(_dir, exist_ok=True) for _dir in dir_list if not os.path.exists(_dir)]
instruments_path = os.path.join(data_dir, 'instruments.csv')  # Unique for each day. No History available.

holidays_23 = ['2023-01-26', '2023-03-07', '2023-03-30', '2023-04-04', '2023-04-07', '2023-04-14', '2023-05-01', '2023-06-29', '2023-08-15', '2023-09-19', '2023-10-02', '2023-10-24', '2023-11-14', '2023-11-27', '2023-12-25']
holidays_24 = ['2024-01-22', '2024-01-26', '2024-03-08', '2024-03-25', '2024-03-29', '2024-04-11', '2024-04-17', '2024-05-01','2024-05-20', '2024-06-17', '2024-07-17', '2024-08-15', '2024-10-02', '2024-11-01', '2024-11-15', '2024-12-25']
holidays_24.append('2024-03-20') #add unusual holidays
holidays = holidays_23 + holidays_24
b_days = pd.bdate_range(start=datetime.now()-relativedelta(months=3), end=datetime.now(), freq='C', weekmask='1111100',
                        holidays=holidays)
b_days = b_days.append(pd.DatetimeIndex([pd.Timestamp(year=2024, month=1, day=20), pd.Timestamp(year=2024, month=3, day=2),
                                          pd.Timestamp(year=2024, month=5, day=18)])) #removed contigency trading drill dates from unusual business days

b_days = b_days[b_days <= datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)].drop_duplicates().sort_values()
today, yesterday = b_days[-1], b_days[-2]
IST = pytz.timezone('Asia/Kolkata')


def define_logger():
    # Logging Definitions
    log_lvl = logging.DEBUG
    console_log_lvl = logging.INFO
    _logger = logging.getLogger('arathi')
    # logger.setLevel(log_lvl)
    _logger.setLevel(console_log_lvl)
    log_file = os.path.join(logs_dir, f'logs_arathi_{datetime.now().strftime("%Y%m%d")}.log')
    handler = TimedRotatingFileHandler(log_file, when='D', delay=True)
    handler.setLevel(log_lvl)
    console = logging.StreamHandler(stream=sys.stdout)
    console.setLevel(console_log_lvl)
    # formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')  #NOSONAR
    # formatter = logging.Formatter('%(asctime)s %(levelname)s %(filename)s %(funcName)s %(message)s')
    formatter = logging.Formatter('%(asctime)s %(levelname)s <%(funcName)s> %(message)s')
    handler.setFormatter(formatter)
    console.setFormatter(formatter)
    _logger.addHandler(handler)  # Comment to disable file logs
    _logger.addHandler(console)
    # logger.propagate = False  # Removes AWS Level Logging as it tracks root propagation as well
    return _logger

# def list_dict():
#     time_list = []
#     try:
#         if type(today.to_pydatetime().date()) == type(datetime.now().date()):
#             start_time = datetime.strptime(f'{today.to_pydatetime().date()} 09:15:00', '%Y-%m-%d %H:%M:%S')
#             end_time = datetime.strptime(f'{today.to_pydatetime().date()} 15:30:00', '%Y-%m-%d %H:%M:%S')
#             print(start_time)
#             interval = timedelta(minutes = 1)
#             current_time = start_time
#
#             while current_time <= end_time:
#                 time_list.append({'ts':current_time, 'strike':0, 'combined_premium':0})
#                 current_time += interval
#
#             return time_list
#     except Exception as e:
#         logger.error(f'Error in list_dict: {str(e)}')
#         return []

def fixed_response_dict():
    time_list = []
    try:
        if type(today) == type(pd.Timestamp(today)):
            start_time = today.replace(hour = 9, minute =15, second =0)
            end_time = today.replace(hour=15, minute=30, second=0)
            # logger.info(f'start time - {start_time}\tend time - {end_time}')
            interval = timedelta(minutes = 1)
            current_time = start_time

            while current_time <= end_time:
                # ts, spot, strike, combined_premium, combined_iv, otm_iv
                time_list.append({'ts':current_time, 'spot':0, 'strike':0, 'combined_premium':0, 'combined_iv':0, 'otm_iv':0, 'prev':False})
                current_time += interval

            return time_list
    except Exception as e:
        logger.error(f'Error in list_dict: {str(e)}')
        return []

logger = define_logger()

