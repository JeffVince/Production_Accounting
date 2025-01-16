import logging
from flask import Blueprint, request, jsonify
from sqlalchemy.exc import IntegrityError

from database_view_util import DatabaseViewUtil

logger = logging.getLogger(__name__)

account_tax_bp = Blueprint("account_tax_bp", __name__)
db_view_util = DatabaseViewUtil()

# ---------------------------------------------------------
# FETCH MAP NAMES
# ---------------------------------------------------------
@account_tax_bp.route("/get_map_names", methods=["GET"])
def get_map_names():
    """
    Return a list of all BudgetMap.map_name values (strings).
    """
    try:
        map_names = db_view_util.fetch_all_map_names()
        return jsonify(map_names), 200
    except Exception as e:
        logger.error(f"Error in get_map_names: {e}", exc_info=True)
        return jsonify([]), 500

# ---------------------------------------------------------
# FETCH MAP DATA
# ---------------------------------------------------------
@account_tax_bp.route("/get_map_data", methods=["GET"])
def get_map_data():
    """
    Return the account + tax records for the specified map_name.
    Query params:
      - map_name
      - page_account (optional)
      - per_page_account (optional)
      - sort_by (optional)
    """
    try:
        map_name = request.args.get("map_name", "")
        page_account = int(request.args.get("page_account", 1))
        per_page_account = int(request.args.get("per_page_account", 40))
        page_tax = 1
        per_page_tax = 10
        sort_by = request.args.get("sort_by", None)

        data = db_view_util.fetch_map_data(
            map_name=map_name,
            page_account=page_account,
            per_page_account=per_page_account,
            page_tax=page_tax,
            per_page_tax=per_page_tax,
            sort_by=sort_by
        )
        return jsonify(data), 200
    except Exception as e:
        logger.error(f"Error in get_map_data: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

# ---------------------------------------------------------
# CREATE MAP
# ---------------------------------------------------------
@account_tax_bp.route("/create_map_code", methods=["POST"])
def create_map_code():
    """
    Create a new BudgetMap entry. Optionally copy from an existing map.
    """
    try:
        body = request.get_json()
        new_map_code = body.get("new_map_code", "")
        copy_from = body.get("copy_from", "")

        if not new_map_code:
            return jsonify({"status": "error", "message": "new_map_code is required"}), 400

        db_view_util.create_map_code(new_map_code, copy_from)
        return jsonify({"status": "success"}), 200
    except Exception as e:
        logger.error(f"Error in create_map_code: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

# ---------------------------------------------------------
# DELETE MAPPING
# ---------------------------------------------------------
@account_tax_bp.route("/delete_mapping", methods=["POST"])
def delete_mapping():
    """
    Delete all AccountCode rows under a given map_name.
    """
    try:
        body = request.get_json()
        map_name = body.get("map_name", "")
        if not map_name:
            return jsonify({"status": "error", "message": "map_name is required"}), 400

        db_view_util.delete_mapping(map_name)
        return jsonify({"status": "success"}), 200
    except ValueError as ve:
        logger.error(f"ValueError in delete_mapping: {ve}", exc_info=True)
        return jsonify({"status": "error", "message": str(ve)}), 400
    except IntegrityError as e:
        logger.error(f"IntegrityError in delete_mapping: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Database integrity error."}), 500
    except Exception as e:
        logger.error(f"Exception in delete_mapping: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

# ---------------------------------------------------------
# CREATE TAX
# ---------------------------------------------------------
@account_tax_bp.route("/create_tax", methods=["POST"])
def create_tax():
    """
    Create a new TaxAccount record. map_name is provided if you
    want to do any special logic (not strictly needed to store the tax).
    """
    try:
        body = request.get_json()
        # map_name = body.get("map_name", "")  # If you want to use it
        tax_code = body.get("tax_code")
        tax_desc = body.get("tax_description", "")

        if not tax_code:
            return jsonify({"status": "error", "message": "tax_code required"}), 400

        new_id = db_view_util.create_tax_record(tax_code, tax_desc)
        return jsonify({"status": "success", "id": new_id}), 200
    except Exception as e:
        logger.error(f"Error in create_tax: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

# ---------------------------------------------------------
# UPDATE TAX
# ---------------------------------------------------------
@account_tax_bp.route("/update_tax", methods=["POST"])
def update_tax():
    """
    Update an existing TaxAccount record's code & description.
    """
    try:
        body = request.get_json()
        # map_name = body.get("map_name", "")  # If needed for logic
        tax_id = body.get("tax_id")
        tax_code = body.get("tax_code")
        tax_desc = body.get("tax_description")

        if not tax_id or not tax_code:
            return jsonify({"status": "error", "message": "tax_id and tax_code are required"}), 400

        db_view_util.update_tax_record(tax_id, tax_code, tax_desc)
        return jsonify({"status": "success"}), 200
    except ValueError as ve:
        logger.error(f"ValueError in update_tax: {ve}", exc_info=True)
        return jsonify({"status": "error", "message": str(ve)}), 400
    except Exception as e:
        logger.error(f"Error in update_tax: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

# ---------------------------------------------------------
# DELETE TAX
# ---------------------------------------------------------
@account_tax_bp.route("/delete_tax", methods=["POST"])
def delete_tax():
    """
    Delete a TaxAccount row by ID, clearing references from any accounts
    in the given map_name.
    """
    try:
        body = request.get_json()
        map_name = body.get("map_name", "")
        tax_id = body.get("tax_id")

        if not map_name or not tax_id:
            return jsonify({"status": "error", "message": "map_name and tax_id required"}), 400

        db_view_util.delete_tax_record(map_name, tax_id)
        return jsonify({"status": "success"}), 200
    except ValueError as ve:
        logger.error(f"ValueError in delete_tax: {ve}", exc_info=True)
        return jsonify({"status": "error", "message": str(ve)}), 400
    except Exception as e:
        logger.error(f"Error in delete_tax: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

# ---------------------------------------------------------
# ASSIGN TAX (SINGLE)
# ---------------------------------------------------------
@account_tax_bp.route("/assign_tax", methods=["POST"])
def assign_tax():
    """
    Assign a single tax_id to a single account in a given map_name.
    """
    try:
        body = request.get_json()
        map_name = body.get("map_name")
        account_id = body.get("account_id")
        tax_id = body.get("tax_id")

        if not map_name or not account_id or not tax_id:
            return jsonify({"status": "error", "message": "map_name, account_id, and tax_id are required"}), 400

        db_view_util.assign_tax_to_account(map_name, account_id, tax_id)
        return jsonify({"status": "success"}), 200
    except ValueError as ve:
        logger.error(f"ValueError in assign_tax: {ve}", exc_info=True)
        return jsonify({"status": "error", "message": str(ve)}), 400
    except Exception as e:
        logger.error(f"Error in assign_tax: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

# ---------------------------------------------------------
# ASSIGN TAX (BULK)
# ---------------------------------------------------------
@account_tax_bp.route("/assign_tax_bulk", methods=["POST"])
def assign_tax_bulk():
    """
    Assigns the same tax_id to multiple account_ids in a given map_name.
    """
    try:
        data = request.get_json()
        map_name = data.get("map_name")
        account_ids = data.get("account_ids", [])
        tax_id = data.get("tax_id")

        if not map_name or not account_ids or not tax_id:
            return jsonify({
                "status": "error",
                "message": "map_name, account_ids, and tax_id are required"
            }), 400

        db_view_util.assign_tax_bulk(map_name, account_ids, tax_id)
        return jsonify({"status": "success"}), 200
    except ValueError as ve:
        logger.error(f"ValueError in assign_tax_bulk: {ve}", exc_info=True)
        return jsonify({"status": "error", "message": str(ve)}), 400
    except IntegrityError as e:
        logger.error(f"IntegrityError in assign_tax_bulk: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Database integrity error."}), 500
    except Exception as e:
        logger.error(f"Exception in assign_tax_bulk: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500