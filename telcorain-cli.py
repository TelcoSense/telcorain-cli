import sys
import argparse
from warnings import simplefilter
from datetime import datetime, timedelta, timezone
from glob import glob
from pathlib import Path
from os.path import exists, join
from concurrent.futures import ThreadPoolExecutor
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


class TelcorainCLI:
    """
    Main class of TelcoRain CLI for raingrids computation.
    """

    delta_map: dict = {
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

    def __init__(
        self,
        config_path: str = "configs/config.ini",
        config_calc_path: str = "configs/config_calc.ini",
        config_db_path: str = "configs/config_db.ini",
    ):
        """Initialize CLI with configuration."""
        self.cp: dict = create_cp_dict(path=config_calc_path, format=True)
        self.config: dict = create_cp_dict(path=config_path, format=False)
        self.config_db: dict = create_cp_dict(path=config_db_path, format=False)

        self.config_path: str = config_path
        self.config_calc_path: str = config_calc_path
        self.config_db_path: str = config_db_path
        self.repetition_interval: int = self.config["setting"]["repetition_interval"]
        self.sleep_interval: int = self.config["setting"]["sleep_interval"]

        self.realtime_timewindow = self.delta_map.get(
            self.cp["realtime"]["realtime_timewindow"]
        ).total_seconds()
        self.sql_man = SqlManager()
        self.influx_man = influx_man
        self.logger = logger

        setup_init_logging(logger, self.config["directories"]["logs"])
        sys.stdout.reconfigure(encoding="utf-8")

    def run(self, first=False):
        """Run the TelcoRain calculation in continuous loop. If first it True, the first iteraction
        is within retention_window interval instead of realtime_timewindow to save comp time.
        """

        if first:
            try:
                # Start the logger
                self._print_init_log_info()
                # Load the link info and select all available links
                links = self.sql_man.load_metadata(
                    min_length=self.cp["cml"]["min_length"], exclude_ids=True
                )
                selected_links = select_all_links(links=links)
                # Get the start time of the application
                start_time = datetime.now(tz=timezone.utc)
                self.logger.info(
                    f"Starting Telcorain CLI at {start_time} for first iteration on retention_window."
                )

                # define calculation class
                calculation = Calculation(
                    influx_man=self.influx_man,
                    links=links,
                    selection=selected_links,
                    cp=self.cp,
                    config=self.config,
                )
                self._run_iteration(
                    calculation, self.cp["realtime"]["retention_window"]
                )
            except KeyboardInterrupt:
                logger.info("Shutdown of the program...")
        try:
            # Start the logger
            self._print_init_log_info()
            # Load the link info and select all available links
            links = self.sql_man.load_metadata(
                min_length=self.cp["cml"]["min_length"], exclude_ids=True
            )
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
            while True:
                self._run_iteration(
                    calculation, self.cp["realtime"]["realtime_timewindow"]
                )
        except KeyboardInterrupt:
            logger.info("Shutdown of the program...")

    def _run_iteration(self, calculation: Calculation, realtime_timewindow: str = "1d"):
        # Calculation runs in a loop every repetition_interval (default 600 seconds)
        self.logger.info(f"Starting new calculation...")

        # Get current time, next iteration time, and previous iteration time
        current_time, next_time, since_time = self._get_times()

        # Cleanup old data
        removed_files, kept_files = self._cleanup_old_files(
            current_time,
            clean_raw=self.config["directories"]["clean_raw"],
            clean_web=self.config["directories"]["clean_web"],
        )
        deleted_rows = self.sql_man.delete_old_data(
            current_time,
            retention_window=self.delta_map.get(
                self.cp["realtime"]["retention_window"]
            ),
        )

        self.logger.info(
            f"Cleanup: Removed {removed_files} files, kept {kept_files}, "
            f"deleted {deleted_rows} DB rows"
        )

        # fetch the data and run the calculation
        calculation.run(realtime_timewindow=realtime_timewindow)

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
            self.realtime_timewindow,
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

        self.logger.info(f"RUN ends. Next iteration should start at: {next_time}.")
        self.logger.info(
            f"Final time of calculation: {datetime.now(tz=timezone.utc) - current_time}"
        )
        self.logger.info(f"...sleeping until {next_time} UTC time...")
        while datetime.now(tz=timezone.utc) < next_time:
            sleep(self.sleep_interval)

    def _print_init_log_info(self):
        """Log initial configuration information."""
        config_info = [
            f"Logger level: {self.config['logging']['init_level']}",
            f"MariaDB: {self.config_db['mariadb']['address']}:{self.config_db['mariadb']['port']}",
            f"InfluxDB: {self.config_db['influx2']['url']}",
            f"Output folders -- log: {self.config['directories']['logs']}",
            f"web: {self.config['directories']['outputs_web']}",
            f"raw: {self.config['directories']['outputs_raw']}",
        ]

        calc_info = [
            f"Step: {self.cp['time']['step']}",
            f"IsMLPEnabled: {self.cp['wet_dry']['is_mlp_enabled']}",
            f"WAA method: {self.cp['waa']['waa_method']}",
            f"Interpolation: res {self.cp['interp']['interp_res']}, power {self.cp['interp']['idw_power']}",
            f"Realtime window: {self.cp['realtime']['realtime_timewindow']}",
            f"Retention window: {self.cp['realtime']['retention_window']}",
        ]

        logger.info("Global config settings: " + "; ".join(config_info))
        logger.info("Calculation settings: " + "; ".join(calc_info))

    def _get_times(self) -> tuple[datetime, datetime, datetime]:
        """Get current, next, and since times for calculation."""
        current_time = datetime.now(tz=timezone.utc)
        return (
            current_time,
            current_time + timedelta(seconds=self.repetition_interval),
            current_time - timedelta(seconds=self.repetition_interval),
        )

    def wipeout_all_data(self):
        """Wipe all data from storage and databases."""
        self.logger.info("[DEVMODE] All calculations will be ERASED from the database.")
        # purge realtime data from MariaDB
        self.sql_man.wipeout_realtime_tables()
        # start thread for wiping out output bucket in InfluxDB
        self.influx_man.run_wipeout_output_bucket()
        # purge raw .npy raingrids and .pngs outputs from disk
        with ThreadPoolExecutor() as executor:
            executor.submit(purge_raw_outputs, config=self.config)
            executor.submit(purge_web_outputs, config=self.config)
        self.logger.info("[DEVMODE] DATA ERASE DONE.")

    def _cleanup_old_files(
        self, current_time: datetime, clean_raw: bool = True, clean_web: bool = True
    ) -> tuple[int, int]:
        """
        Delete old files from raw and/or web output directories.
        Files are deleted if their timestamp (parsed from filename) is older than the retention threshold.

        Parameters:
            current_time (datetime): The reference time (usually now).
            clean_raw (bool): Whether to clean the raw output folder.
            clean_web (bool): Whether to clean the web output folder.

        Returns:
            Tuple[int, int]: Number of deleted files, number of kept files.
        """
        retention_window = self.cp["realtime"]["retention_window"]
        threshold = current_time - self.delta_map.get(retention_window)

        deleted_count = 0
        kept_count = 0

        def delete_in_folder(folder: Path) -> tuple[int, int]:
            deleted, kept = 0, 0
            for file_path in folder.glob("*"):
                if not file_path.is_file():
                    continue
                try:
                    file_time = datetime.strptime(
                        file_path.stem, "%Y-%m-%d_%H%M"
                    ).replace(tzinfo=timezone.utc)
                    if file_time < threshold:
                        file_path.unlink()
                        deleted += 1
                    else:
                        kept += 1
                except ValueError:
                    logger.warning(f"Skipping non-timestamped file: {file_path.name}")
            return deleted, kept

        if clean_raw:
            raw_dir = Path(self.config["directories"]["outputs_raw"])
            d, k = delete_in_folder(raw_dir)
            deleted_count += d
            kept_count += k

        if clean_web:
            web_dir = Path(self.config["directories"]["outputs_web"])
            d, k = delete_in_folder(web_dir)
            deleted_count += d
            kept_count += k

        return deleted_count, kept_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="TelcoRain CLI. It computes raingrids from CML Influx data, saves results"
        "to a local folder (.npy and .png), MariaDB (param info and data info), and InfluxDB (rainrates)."
    )

    parser.add_argument(
        "--run", action="store_true", default=True, help="Run the CLI calculation."
    )

    parser.add_argument(
        "--first",
        action="store_true",
        default=False,
        help="Run with the retention_window first and then with realtime_timewindow.",
    )

    parser.add_argument(
        "--wipe_all",
        action="store_true",
        default=False,
        help="Wipe all data from local storage, MariaDB, and InfluxDB (rain rates).",
    )

    args = parser.parse_args()
    telco_cli = TelcorainCLI()
    if args.wipe_all:
        telco_cli.wipeout_all_data()
    if args.first:
        telco_cli.run(first=True)
    if args.run:
        telco_cli.run()
