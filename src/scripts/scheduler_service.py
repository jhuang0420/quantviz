import servicemanager
import win32serviceutil
import win32service
import win32event
import threading
import time
import logging
import sys
from pathlib import Path
import win32api
win32api.SetConsoleCtrlHandler(lambda x: True, True)

# Configure logging
LOG_FILE = Path(__file__).parent / 'logs' / 'scheduler_service.log'
LOG_FILE.parent.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

class SchedulerService(win32serviceutil.ServiceFramework):
    _svc_name_ = "QuantVizScheduler"
    _svc_display_name_ = "QuantViz Data Scheduler"
    _svc_description_ = "Automatically updates financial data for QuantViz dashboard"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.is_running = False

    def SvcDoRun(self):
        self.is_running = True
        logging.info("Service starting...")
        
        try:
            from src.scripts.scheduler import daily_update
        except ImportError as e:
            logging.error(f"Import error: {e}")
            return

        while self.is_running:
            try:
                logging.info("Running daily update...")
                daily_update()
            except Exception as e:
                logging.error(f"Update failed: {e}")
            
            # Wait 60 seconds or until stop signal
            wait_result = win32event.WaitForSingleObject(
                self.hWaitStop, 
                60000  # 60 seconds
            )
            
            if wait_result == win32event.WAIT_OBJECT_0:
                break

    def SvcStop(self):
        self.is_running = False
        logging.info("Service stopping...")
        win32event.SetEvent(self.hWaitStop)

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(SchedulerService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(SchedulerService)