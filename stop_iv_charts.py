import psutil


def find_and_terminate(script_path):
    for process in psutil.process_iter(['pid', 'name', 'cmdline']):
        # print(process.info)
        try:
            # if process.info['pid'] == 14756:
            #     print()

            # if script_path in process.info['cmdline']:
            #     process.terminate()
            #     print(f'Terminated process {process.info["pid"]} running at {script_path}')
            #     return
            # elif process.info['name'] in ['python.exe', 'python', 'python3']:
            #     for temp in process.info['cmdline']:
            #         if temp in ['main.py', 'app.py']:
            #             process.terminate()
            #             print(f'Terminated python script {process.info["pid"]} running at {script_path}')
            #             return
            if process.info['name'] == 'OpenConsole.exe':
                process.terminate()
                print(f'Terminated console script {process.info["pid"]} running at {script_path}')
                return
        except Exception:
            pass
            # print(f'Exception')
    print(f'No running process found at {script_path}')

def kill_process_by_port(port):
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            connection_info = proc.connections()
            for conn in connection_info:
                if conn.laddr.port == port:
                    proc.kill()
                    print(f"Process with PID {proc.pid} killed.")
                    return
        except Exception as e:
            print(e)
    print("No process found running on port", port)


if __name__ == '__main__':
    # script_path = "app.py"
    script_path = "C:\\Users\\colo\\iv_charts\\july82024_New\\data_analytics_backend\\app.bat"
    find_and_terminate(script_path)
    #
    # # script_path = "main.py"
    script_path = "C:\\Users\\colo\\iv_charts\\july82024_New\\data_analytics_backend\\main.bat"
    find_and_terminate(script_path)

    script_path = "C:\\Users\\colo\\iv_charts\\july82024_New\\data_analytics_frontend\\start_prog.bat"
    find_and_terminate(script_path)
