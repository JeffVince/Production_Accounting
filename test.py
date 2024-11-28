from monday_files import MondayClient, monday_service
from monday_files.monday_api import MondayAPI




Service = monday_service.MondayService()
client = MondayClient(Service.api_token)
API = MondayAPI()


Service.sync_sub_items_from_monday_board()



#API.sync_main_items_from_monday_board()
