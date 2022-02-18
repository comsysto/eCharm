import sys
from configparser import ConfigParser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pipelines._bna import BnaPipeline
from settings import db_uri
from utils.logging_utils import log

if __name__ == '__main__':
    try:
        #Read config.ini file
        config = ConfigParser()
        config.read("config/config.ini")
        
        engine = create_engine(db_uri, echo=True)
        Session = sessionmaker(bind=engine)
        db_session = Session()

        bna_pipeline = BnaPipeline(config, db_session)
        bna_pipeline.run()

    except Exception as e:
            log.error("Something went wrong! Exit!", e)
            sys.exit() # Maybe a little bit to harsh
