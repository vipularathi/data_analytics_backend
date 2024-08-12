# # import psutil
# #
# # # List of process names to stop
# # process_names = [
# #     "app.py",  # Corresponds to app.bat
# #     "app_old.py",  # Corresponds to app_old.bat
# #     "npm",  # Corresponds to start_frontend.bat
# #     "main.py", # Corresponds to main.bat
# # ]
# #
# # def kill_process_tree(pid):
# #     """Kills a process tree starting from the given pid."""
# #     try:
# #         parent = psutil.Process(pid)
# #         children = parent.children(recursive=True)
# #         for child in children:
# #             child.terminate()
# #         parent.terminate()
# #
# #         # Wait for process termination (with timeout)
# #         psutil.wait_procs(children, timeout=5)
# #         parent.wait(timeout=5)
# #     except psutil.NoSuchProcess:
# #         pass
# #
# # def stop_processes(process_names):
# #     """Stops processes with names specified in process_names."""
# #     for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
# #         try:
# #             cmdline = proc.info['cmdline']
# #             if cmdline and any(proc_name in cmdline for proc_name in process_names):
# #                 print(f"Terminating process {proc.info['name']} (PID: {proc.info['pid']})")
# #                 kill_process_tree(proc.info['pid'])
# #         except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
# #             continue
# #
# # if __name__ == "__main__":
# #     stop_processes(process_names)
# #     print("All specified processes have been terminated.")
#
# import psutil
# import os
#
# # List of process names or keywords to stop
# process_names = [
#     "app.py",        # Corresponds to app.bat
#     "app_old.py",    # Corresponds to app_old.bat
#     "node",          # Typically the process that runs "npm run dev"
#     "main.py",       # Corresponds to main.bat
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
#     for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'ppid']):
#         try:
#             cmdline = proc.info['cmdline']
#             if cmdline and any(proc_name in cmdline for proc_name in process_names):
#                 print(f"Terminating process {proc.info['name']} (PID: {proc.info['pid']})")
#                 kill_process_tree(proc.info['pid'])
#
#                 # Also terminate the parent process to close the terminal window
#                 parent_proc = psutil.Process(proc.info['ppid'])
#                 if parent_proc.is_running() and parent_proc.name() in ['cmd.exe', 'python.exe', 'powershell.exe']:
#                     print(f"Terminating parent process {parent_proc.name()} (PID: {parent_proc.pid}) to close terminal window")
#                     kill_process_tree(parent_proc.pid)
#
#         except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
#             continue
#
# if __name__ == "__main__":
#     stop_processes(process_names)
#     print("All specified processes and their terminal windows have been terminated.")

import psutil

# List of process names or keywords to stop
process_names = [
    "node",          # corresponding to start_frontend.bat
    "main.py",       # Corresponds to main.bat
    "app.py",        # Corresponds to app.bat
    "app_old.py"    # Corresponds to app_old.bat
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

