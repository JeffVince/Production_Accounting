# test_monday_api.py
import pytest
from unittest.mock import patch, MagicMock
from monday_files.monday_api import MondayAPI

@pytest.fixture
def monday_api_instance():
    return MondayAPI()

def test_create_item(monday_api_instance):
    with patch('requests.post') as mock_post:
        # Mock a successful JSON response
        mock_post.return_value.raise_for_status = MagicMock()
        mock_post.return_value.json.return_value = {
            "data": {
                "create_item": {"id": "12345"},
                "complexity": {"query": 100, "before": 10000, "after": 9900}
            }
        }

        response = monday_api_instance.create_item(board_id=123, group_id="topics", name="Test Item", column_values={"status": "Done"})
        assert response["data"]["create_item"]["id"] == "12345"

def test_fetch_all_items(monday_api_instance):
    with patch('requests.post') as mock_post:
        # Simulate a scenario with items_page returned
        mock_post.return_value.raise_for_status = MagicMock()
        mock_post.return_value.json.side_effect = [
            {
                "data": {
                    "boards": [{
                        "items_page": {
                            "cursor": "next_cursor",
                            "items": [
                                {"id": "1", "name": "Item 1", "column_values": []}
                            ]
                        }
                    }],
                    "complexity": {"query": 50, "before": 10000, "after": 9950}
                }
            },
            {
                "data": {
                    "next_items_page": {
                        "cursor": None,
                        "items": [
                            {"id": "2", "name": "Item 2", "column_values": []}
                        ]
                    },
                    "complexity": {"query": 50, "before": 9950, "after": 9900}
                }
            }
        ]

        items = monday_api_instance.fetch_all_items(board_id=123)
        assert len(items) == 2
        assert items[0]["id"] == "1"
        assert items[1]["id"] == "2"

def test_find_or_create_contact_in_monday(monday_api_instance):
    with patch.object(monday_api_instance, 'fetch_contact_by_name') as mock_fetch_contact, \
         patch.object(monday_api_instance, 'create_contact') as mock_create_contact:
        # Test when contact exists
        mock_fetch_contact.return_value = {"id": "999", "name": "Existing Contact"}
        result = monday_api_instance.create_contact_in_monday("Existing Contact")
        assert result == {"id": "999", "name": "Existing Contact"}

        # Test when contact doesn't exist
        mock_fetch_contact.return_value = None
        mock_create_contact.return_value = {
            "data": {
                "create_item": {
                    "id": "1010",
                    "name": "New Contact"
                }
            }
        }
        result = monday_api_instance.create_contact_in_monday("New Contact")
        assert result == {"id": "1010", "name": "New Contact"}