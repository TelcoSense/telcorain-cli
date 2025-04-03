import sys
import argparse
from warnings import simplefilter
from datetime import datetime, timedelta, timezone
from glob import glob
from pathlib import Path
from os.path import exists, join
from os import remove
from time import sleep
from sys import stdout, stderr

from telcorain.database.influx_manager import influx_man
from telcorain.database.sql_manager import SqlManager
from telcorain.handlers.logging_handler import setup_init_logging, logger
from telcorain.handlers.writer import (
    RealtimeWriter,
    purge_raw_outputs,
    purge_web_outputs,
)
from telcorain.procedures.calculation import Calculation
from telcorain.procedures.utils.helpers import create_cp_dict, select_all_links


simplefilter(action="ignore", category=FutureWarning)


# TODO: concat a erase dat z influx_data misto znovupocitani all the time
# TODO: reformat kodu


class TelcorainCLI:
    """
    This is the main class of TelcoRain CLI for raingrids computation.
    """

    def __init__(
        self,
        config_path: str = "configs/config.ini",
        config_calc_path: str = "configs/config_calc.ini",
        config_db_path: str = "configs/config_db.ini",
    ):
        self.config_path: str = config_path
        self.config_calc_path: str = config_calc_path
        self.config_db_path: str = config_db_path
        self.cp: dict = create_cp_dict(path=config_calc_path, format=True)
        self.config: dict = create_cp_dict(path=config_path, format=False)
        self.config_db: dict = create_cp_dict(path=config_db_path, format=False)
        self.repetition_interval: int = self.config["setting"]["repetition_interval"]
        self.sleep_interval: int = self.config["setting"]["sleep_interval"]
        self.delta_map: dict = {
            "1h": timedelta(hours=1),
            "3h": timedelta(hours=3),
            "6h": timedelta(hours=6),
            "12h": timedelta(hours=12),
            "1d": timedelta(days=1),
            "2d": timedelta(days=2),
            "7d": timedelta(days=7),
            "14d": timedelta(days=14),
            "30d": timedelta(days=30),
        }

        self.retention = self.delta_map.get(
            self.cp["realtime"]["realtime_timewindow"]
        ).total_seconds()
        self.sql_man = SqlManager(min_length=self.cp["cml"]["min_length"])
        self.influx_man = influx_man
        self.logger = logger
        setup_init_logging(logger, self.config["directories"]["logs"])
        sys.stdout.reconfigure(encoding="utf-8")

    def run(self):
        # Run the TelcoRain calculation
        self._run_telcorain_calculation()

    def _run_telcorain_calculation(self):
        try:
            # Start the logger
            self._print_init_log_info()
            # Load the link info and select all available links
            links = self.sql_man.load_metadata()
            selected_links = select_all_links(links=links)
            # Get the start time of the application
            start_time = datetime.now(tz=timezone.utc)
            self.logger.info(f"Starting Telcorain CLI at {start_time}.")

            # define calculation class
            calculation = Calculation(
                influx_man=self.influx_man,
                links=links,
                selection=selected_links,
                cp=self.cp,
                config=self.config,
            )

            # Calculation runs in a loop every repetition_interval (default 600 seconds)
            running = True
            while running:
                self.logger.info(f"Starting new calculation...")

                # Get current time, next iteration time, and previous iteration time
                current_time, next_time, since_time = self._get_times()

                # remove old local files and database entries
                total_of_removed_files, total_of_keep_files = self._check_local_files(
                    current_time=current_time
                )
                total_delete_rows = self.sql_man.delete_old_data(
                    current_time,
                    retention_window=self.delta_map.get(
                        self.cp["realtime"]["realtime_timewindow"]
                    ),
                )

                self.logger.info(f"Deleting old files based on realtime_window: ")
                self.logger.info(
                    f"Total of: {total_of_removed_files} files deleted from local output."
                )
                self.logger.info(
                    f"Total of: {total_of_keep_files} files remained in local output."
                )
                self.logger.info(
                    f"Total of: {total_delete_rows} rows deleted from MariaDB."
                )

                # fetch the data and run the calculation
                calculation.run()

                # create the realtime writer object
                writer = RealtimeWriter(
                    sql_man=self.sql_man,
                    influx_man=self.influx_man,
                    skip_influx=self.cp["realtime"]["is_influx_write_skipped"],
                    skip_sql=self.cp["realtime"]["is_sql_write_skipped"],
                    since_time=since_time,
                    cp=self.cp,
                    config=self.config,
                )

                # write to the SQL
                self.sql_man.insert_realtime(
                    self.retention,
                    self.repetition_interval,
                    self.cp["interp"]["interp_res"],
                    self.cp["limits"]["x_min"],
                    self.cp["limits"]["x_max"],
                    self.cp["limits"]["y_min"],
                    self.cp["limits"]["y_max"],
                    self.config_db["http"]["http_server_address"],
                    self.config_db["http"]["http_server_port"],
                )

                # write to the local storage
                writer.push_results(
                    rain_grids=calculation.rain_grids,
                    x_grid=calculation.x_grid,
                    y_grid=calculation.y_grid,
                    calc_dataset=calculation.calc_data_steps,
                )

                self.logger.info(
                    f"RUN ends. Next iteration should start at: {next_time}."
                )
                self.logger.info(
                    f"Final time of calculation: {datetime.now(tz=timezone.utc) - current_time}"
                )
                self.logger.info(f"...sleeping until {next_time}...")
                while datetime.now(tz=timezone.utc) < next_time:
                    sleep(self.sleep_interval)
        except KeyboardInterrupt:
            self.logger.info("Shutdown of the program...")
            return

    def _print_init_log_info(self):
        # Print the logger info at the start of the application
        self.logger.info(
            f"Global config settings: "
            f"Logger level: {self.config['logging']['init_level']}; "
            f"MariaDB IP: {self.config_db['mariadb']['address']}, port: {self.config_db['mariadb']['port']}; "
            f"InfluxDB IP/port: {self.config_db['influx2']['url']}; "
            # f"HTTP server IP: {self.config_db['http']['http_server_address']}, port: {self.config_db['http']['http_server_port']}; "
            f"Output folders: logs: {self.config['directories']['logs']}, web: {self.config['directories']['outputs_web']}, raw: {self.config['directories']['outputs_raw']}"
        )
        self.logger.info(
            f"Calculation config settings: "
            f"Step: {self.cp['time']['step']}; "
            f"IsMLPEnabled: {self.cp['wet_dry']['is_mlp_enabled']}; "
            f"WAA Schleiss: val: {self.cp['waa']['waa_schleiss_val']}, {self.cp['waa']['waa_schleiss_tau']}; "
            f"Interpolation: res: {self.cp['interp']['interp_res']}, power: {self.cp['interp']['idw_power']}; "
            f"near: {self.cp['interp']['idw_near']}, dist: {self.cp['interp']['idw_dist']}; "
            f"Realtime window: {self.cp['realtime']['realtime_timewindow']}; "
            f"Realtime optimization enabled: {self.cp['realtime']['realtime_optimization']}"
        )

    def _get_times(self):
        # Get current time, time of the next iteration and tim from the
        current_time = datetime.now(tz=timezone.utc)
        next_time = current_time + timedelta(seconds=self.repetition_interval)
        since_time = current_time - timedelta(seconds=self.repetition_interval)

        return current_time, next_time, since_time

    def _get_last_record(self):
        # Get the last record from MariaDB
        last_record = self.sql_man.get_last_raingrid()
        if len(last_record) > 0:
            # get the last datetime from the database
            last_time = list(last_record.keys())[0]
        else:
            # create datetime that is always older than realtime_window
            last_time = datetime(2021, 1, 1, 1, 0, 0)

        return last_time

    def wipeout_all_data(self):
        # Wipeout all data from local storage, MariaDB, and InfluxDB
        self.logger.info("[DEVMODE] All calculations will be ERASED from the database.")
        # purge realtime data from MariaDB
        self.sql_man.wipeout_realtime_tables()
        # start thread for wiping out output bucket in InfluxDB
        self.influx_man.run_wipeout_output_bucket()
        # purge raw .npy raingrids and .pngs outputs from disk
        purge_raw_outputs(config=self.config)
        purge_web_outputs(config=self.config)
        self.logger.info("[DEVMODE] ERASE DONE.")

    def _check_local_files(self, current_time: datetime):
        # Check files in the output folder. Delete older files than retention_window.
        retention_window = self.cp["realtime"]["realtime_timewindow"]
        output_raw_dir = self.config["directories"]["outputs_raw"]
        output_web_dir = self.config["directories"]["outputs_web"]

        # Get all files in the output_raw_dir
        all_current_files = glob(
            join(output_raw_dir, "*.*")
        )  # Match all files with extensions

        # Parse filenames to datetimes
        all_current_files_times = []
        for filename in all_current_files:
            try:
                # Extract the stem (filename without extension)
                stem = Path(filename).stem
                dt = datetime.strptime(stem, "%Y-%m-%d_%H%M")
                # Ensure datetime is timezone-aware (UTC)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                all_current_files_times.append((filename, dt))
            except ValueError as e:
                self.logger.error(
                    f"Skipping file {filename}: could not parse datetime from filename. Error: {e}"
                )

        # Calculate the threshold for deletion
        threshold = current_time - self.delta_map.get(retention_window)

        # Separate files to keep and delete
        files_to_keep = [
            filename for filename, dt in all_current_files_times if dt >= threshold
        ]
        files_to_delete = [
            filename for filename, dt in all_current_files_times if dt < threshold
        ]

        # Delete files older than the threshold
        removed_files = []
        for filename in files_to_delete:
            try:
                # Construct paths for raw and web files
                stem = Path(filename).stem
                filepath_raw = join(output_raw_dir, f"{stem}.npy")
                filepath_web = join(output_web_dir, f"{stem}.png")

                # Delete files if they exist
                if exists(filepath_raw):
                    remove(filepath_raw)
                    self.logger.debug(f"Deleted raw file: {filepath_raw}")
                if exists(filepath_web):
                    remove(filepath_web)
                    self.logger.debug(f"Deleted web file: {filepath_web}")

                removed_files.append(filename)
            except Exception as e:
                self.logger.error(f"Error deleting {filename}: {e}")

        return len(removed_files), len(files_to_keep)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="TelcoRain CLI. It computes raingrids from CML Influx data, saves results"
        "to a local folder (.npy and .png), MariaDB (param info and data info), and InfluxDB (rainrates)."
    )

    parser.add_argument(
        "--run", action="store_true", default=False, help="Run the CLI calculation."
    )

    parser.add_argument(
        "--wipe_all",
        action="store_true",
        default=False,
        help="Wipe all data from local storage, MariaDB, and InfluxDB.",
    )

    args = parser.parse_args()
    telco_cli = TelcorainCLI()
    if args.wipe_all:
        telco_cli.wipeout_all_data()
    if args.run:
        telco_cli.run()
