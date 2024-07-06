import psutil


def find_and_terminate(script_path):
    for process in psutil.process_iter(['pid', 'name', 'cmdline']):
        if process.info['name'] == 'python.exe' and script_path in process.info['cmdline']:
            process.terminate()
            print(f'Terminated process {process.info["pid"]} running at {script_path}')
            return
    print(f'No running process found at {script_path}')

if __name__ == '__main__':
    script_path = r"C:\Users\colo\iv_charts\new_backend_with_login\main.bat"
    find_and_terminate(script_path)
    script_path = r"C:\Users\colo\iv_charts\new_backend_with_login\app.bat"
    find_and_terminate(script_path)