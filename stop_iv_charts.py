# # import psutil
# #
# #
# # def find_and_terminate(script_path):
# #     for process in psutil.process_iter(['pid', 'name', 'cmdline']):
# #         # print(process.info)
# #         try:
# #             # if process.info['pid'] == 14756:
# #             #     print()
# #
# #             # if script_path in process.info['cmdline']:
# #             #     process.terminate()
# #             #     print(f'Terminated process {process.info["pid"]} running at {script_path}')
# #             #     return
# #             # elif process.info['name'] in ['python.exe', 'python', 'python3']:
# #             #     for temp in process.info['cmdline']:
# #             #         if temp in ['main.py', 'app.py']:
# #             #             process.terminate()
# #             #             print(f'Terminated python script {process.info["pid"]} running at {script_path}')
# #             #             return
# #             if process.info['name'] == 'OpenConsole.exe':
# #                 process.terminate()
# #                 print(f'Terminated console script {process.info["pid"]} running at {script_path}')
# #                 return
# #         except Exception:
# #             pass
# #             # print(f'Exception')
# #     print(f'No running process found at {script_path}')
# #
# # def kill_process_by_port(port):
# #     for proc in psutil.process_iter(['pid', 'name']):
# #         try:
# #             connection_info = proc.connections()
# #             for conn in connection_info:
# #                 if conn.laddr.port == port:
# #                     proc.kill()
# #                     print(f"Process with PID {proc.pid} killed.")
# #                     return
# #         except Exception as e:
# #             print(e)
# #     print("No process found running on port", port)
# #
# #
# # if __name__ == '__main__':
# #     # script_path = "app.py"
# #     script_path = "C:\\Users\\colo\\iv_charts\\july82024_New\\data_analytics_backend\\app.bat"
# #     find_and_terminate(script_path)
# #     #
# #     # # script_path = "main.py"
# #     script_path = "C:\\Users\\colo\\iv_charts\\july82024_New\\data_analytics_backend\\main.bat"
# #     find_and_terminate(script_path)
# #
# #     script_path = "C:\\Users\\colo\\iv_charts\\july82024_New\\data_analytics_frontend\\start_prog.bat"
# #     find_and_terminate(script_path)
#
# import psutil
# import os
# import signal
#
# # List of process names to stop
# process_names = [
#     "app.py",  # Corresponds to app.bat
#     "app_old.py",  # Corresponds to app_old.bat
#     "npm"  # Corresponds to start_frontend.bat
# ]
#
# def kill_process_tree(pid):
#     """Kills a process tree starting from the given pid."""
#     try:
#         parent = psutil.Process(pid)
#         children = parent.children(recursive=True)
#         for child in children:
#             child.terminate()
#         parent.terminate()
#
#         # Wait for process termination (with timeout)
#         psutil.wait_procs(children, timeout=5)
#         parent.wait(timeout=5)
#     except psutil.NoSuchProcess:
#         pass
#
# def stop_processes(process_names):
#     """Stops processes with names specified in process_names."""
#     for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
#         try:
#             if any(proc_name in proc.info['cmdline'] for proc_name in process_names):
#                 print(f"Terminating process {proc.info['name']} (PID: {proc.info['pid']})")
#                 kill_process_tree(proc.info['pid'])
#         except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
#             continue
#
# if __name__ == "__main__":
#     stop_processes(process_names)
#     print("All specified processes have been terminated.")

import psutil

# List of process names or keywords to stop
process_names = [
    "app.py",        # Corresponds to app.bat
    "app_old.py",    # Corresponds to app_old.bat
    "node",          # Typically the process that runs "npm run dev"
    "main.py"       # Corresponds to main.bat
]

def kill_process_and_terminal(pid):
    """Kills a process and attempts to kill its parent terminal process."""
    try:
        proc = psutil.Process(pid)
        parent_proc = proc.parent()

        # Kill the process
        proc.kill()
        proc.wait(timeout=5)

        # Attempt to kill the terminal window if it's a known terminal process
        if parent_proc and parent_proc.name().lower() in ['cmd.exe', 'powershell.exe', 'conhost.exe']:
            print(f"Terminating parent terminal process {parent_proc.name()} (PID: {parent_proc.pid})")
            parent_proc.terminate()
            parent_proc.wait(timeout=5)

    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        pass

def stop_processes(process_names):
    """Stops processes with names specified in process_names and closes their terminal windows."""
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info['cmdline']
            if cmdline and any(proc_name in cmdline for proc_name in process_names):
                print(f"Terminating process {proc.info['name']} (PID: {proc.info['pid']})")
                kill_process_and_terminal(proc.info['pid'])
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

if __name__ == "__main__":
    stop_processes(process_names)
    print("All specified processes and their terminal windows have been terminated.")
