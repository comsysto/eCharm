from sqlalchemy.engine import create_engine
from sqlalchemy.orm.scoping import ScopedSession
from sqlalchemy.orm.session import sessionmaker
from settings import db_uri

engine = create_engine(db_uri)
Session = ScopedSession(sessionmaker())
Session.configure(bind=engine)