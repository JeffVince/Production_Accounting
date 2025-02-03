# test_monday_api.py
import pytest
from unittest.mock import MagicMock
from monday_api import MondayAPI

class TestMondayAPI:
    @pytest.fixture(autouse=True)
    def setup_api(self):
        # Instantiate MondayAPI and patch the internal _make_request method.
        self.mon_api = MondayAPI()
        self.mon_api._make_request = MagicMock()

    def test_create_item_success(self):
        fake_response = {"data": {"create_item": {"id": "12345"}}}
        self.mon_api._make_request.return_value = fake_response

        board_id = 1001
        group_id = "groupA"
        name = "Test Item"
        column_values = {"col1": "val1"}
        result = self.mon_api.create_item(board_id, group_id, name, column_values)
        self.mon_api._make_request.assert_called_once()
        assert result == fake_response

    def test_create_subitem_success(self):
        fake_response = {"data": {"create_subitem": {"id": "sub123"}}}
        self.mon_api._make_request.return_value = fake_response

        parent_item_id = 2002
        subitem_name = "Subitem"
        column_values = {"subcol": "subval"}
        result = self.mon_api.create_subitem(parent_item_id, subitem_name, column_values)
        self.mon_api._make_request.assert_called_once()
        assert result == fake_response

    def test_create_contact_success(self):
        fake_response = {"data": {"create_item": {"id": "cont123", "name": "Contact Name"}}}
        self.mon_api._make_request.return_value = fake_response

        result = self.mon_api.create_contact("Contact Name")
        self.mon_api._make_request.assert_called_once()
        assert result == fake_response

    def test_update_item_success(self):
        fake_response = {"data": {"change_multiple_column_values": {"id": "upd123"}}}
        self.mon_api._make_request.return_value = fake_response

        result = self.mon_api.update_item("upd123", {"col": "new_val"}, type="contact")
        self.mon_api._make_request.assert_called_once()
        assert result == fake_response

    def test_fetch_all_items_single_page(self):
        # Simulate a single-page response with no cursor.
        fake_page = {
            "data": {
                "boards": [{
                    "items_page": {
                        "cursor": None,
                        "items": [{"id": "1", "name": "Item1", "column_values": []}]
                    }
                }]
            }
        }
        self.mon_api._make_request.return_value = fake_page
        items = self.mon_api.fetch_all_items(board_id=1001, limit=10)
        assert len(items) == 1
        assert items[0]["id"] == "1"

    def test_fetch_all_items_multiple_pages(self):
        # Simulate two pages using side_effect.
        first_page = {
            "data": {
                "boards": [{
                    "items_page": {
                        "cursor": "cursor1",
                        "items": [{"id": "1", "name": "Item1", "column_values": []}]
                    }
                }]
            }
        }
        second_page = {
            "data": {
                "next_items_page": {
                    "cursor": None,
                    "items": [{"id": "2", "name": "Item2", "column_values": []}]
                }
            }
        }
        self.mon_api._make_request.side_effect = [first_page, second_page]
        items = self.mon_api.fetch_all_items(board_id=1001, limit=10)
        assert len(items) == 2
        ids = [item["id"] for item in items]
        assert "1" in ids and "2" in ids