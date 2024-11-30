from config import Config
from db_util import initialize_database
from po_log_processor import parse_po_log_main_items, parse_po_log_sub_items, get_contacts_list
from po_log_database_util import PoLogDatabaseUtil

P = PoLogDatabaseUtil()
# Initialize the database
config = Config()
db_settings = config.get_database_settings()
initialize_database(db_settings['url'])


filename = "data.txt"

main = parse_po_log_main_items(filename)

detail = parse_po_log_sub_items(filename)

contacts = get_contacts_list(main, detail)


new_list = P.get_contact_surrogate_ids(contacts)