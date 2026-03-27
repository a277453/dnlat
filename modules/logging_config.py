import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zipfile import ZipFile
from datetime import datetime


log_folder = Path(__file__).parent
main_log_file = log_folder / "app.log"

logger = logging.getLogger("app_logger")
logger.setLevel(logging.DEBUG)
logger.propagate = False

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] [%(name)s:%(module)s] %(message)s"
)

class CustomRotatingFileHandler(RotatingFileHandler):

    def doRollover(self):
        # Close the current file
        if self.stream:
            self.stream.close()
            self.stream = None

        # Find the next available slot (app1.log → app5.log)
        for i in range(1, 6):
            target = log_folder / f"app{i}.log"
            if not target.exists():
                (log_folder / "app.log").rename(target)
                break

        # Reopen app.log as the new active file
        self.stream = self._open()

        # Check if all 5 rotated files now exist → zip them
        rotated_files = [log_folder / f"app{i}.log" for i in range(1, 6)]
        if all(f.exists() for f in rotated_files):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_name = log_folder / f"App_Logs_{timestamp}.zip"

            with ZipFile(zip_name, 'w') as zipf:
                for f in rotated_files:
                    zipf.write(f, arcname=f.name)
                    f.unlink()

            logger.info("Zipped logs into %s and deleted app1.log–app5.log", zip_name)


# Use Custom Handler
file_handler = CustomRotatingFileHandler(
    main_log_file,
    maxBytes=5_000_000,
    backupCount=5
)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

logger.info("Logger initialized at %s", main_log_file)