from mapping.charging import map_charging_bna
from mapping.stations import map_address_bna, map_stations_bna
from services.excel_file_loader_service import ExcelFileLoaderService
from tqdm import tqdm
from utils.logging_utils import log

class BnaPipeline:
    def __init__(self, config, db_session):
        self.config = config
        self.db_session = db_session
        self.excel_file_loader_service = ExcelFileLoaderService()
        self.excel_df = self.excel_file_loader_service.load(config["EXCEL"]["path"])

    def run(self):
            try: 
                for _, row in tqdm(self.excel_df.iterrows()):
                    mapped_station = map_stations_bna(row)
                    mapped_address = map_address_bna(row, mapped_station.id)
                    mapped_charging = map_charging_bna(row, mapped_station.id)
                    self.db_session.bulk_save_objects([mapped_station, mapped_address,mapped_charging])
                    self.db_session.commit()
            except Exception as e:
                log.error("BNA pipeline failed to run.", e)
                self.db_session.rollback()
