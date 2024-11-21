from database.base import Base
from database.db_util import initialize_database

# Initialize database connection
engine = initialize_database("sqlite:////Users/haske107/PycharmProjects/Dropbox Listener/database/main_db.sqlite")

# Create tables
Base.metadata.create_all(engine)