from database.base import Base
from database.db_util import initialize_database
engine = initialize_database('sqlite:////Users/haske107/PycharmProjects/Dropbox Listener/database/main_db.sqlite')
Base.metadata.create_all(engine)