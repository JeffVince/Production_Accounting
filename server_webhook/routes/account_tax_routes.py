import logging
from flask import Blueprint, request, jsonify
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from server_webhook.models.account_tax_model import AccountTaxModel

logger = logging.getLogger(__name__)
account_tax_bp = Blueprint("account_tax_bp", __name__)
db_view_util = AccountTaxModel()


@account_tax_bp.route("/get_map_names", methods=["GET"])
def get_map_names():
    try:
        names = db_view_util.fetch_all_map_names()
        return jsonify(names), 200
    except SQLAlchemyError as e:
        logger.exception("get_map_names error:")
        return jsonify([]), 500


@account_tax_bp.route("/get_map_data", methods=["GET"])
def get_map_data():
    """
    Expects ?map_name=xxx & ledger_id=???
    Also ?sort_by=some_col_asc or some_col_desc
    like 'code_natural_asc', 'linked_tax_desc', 'updated_asc', etc.
    """
    try:
        map_name = request.args.get("map_name","").strip()
        ledger_id = request.args.get("ledger_id","").strip()
        page_account = int(request.args.get("page_account","1"))
        per_page_account = int(request.args.get("per_page_account","40"))
        raw_sort = request.args.get("sort_by","code_natural_asc")
        parts=raw_sort.split("_")
        if len(parts)==2:
            sort_col, direction = parts
        else:
            sort_col, direction = ("code_natural","asc")

        data = db_view_util.fetch_map_data(
            map_name=map_name,
            page_account=page_account,
            per_page_account=per_page_account,
            ledger_id=ledger_id,
            sort_by=sort_col,
            direction=direction
        )
        return jsonify(data),200
    except SQLAlchemyError as e:
        logger.exception("get_map_data error:")
        return jsonify({"error":str(e)}),500


@account_tax_bp.route("/create_map_code", methods=["POST"])
def create_map_code():
    """
    JSON: { new_map_code: "SomeMap", copy_from: "OldMap" }
    """
    try:
        body = request.get_json()
        new_map = body.get("new_map_code","").strip()
        copy_from = body.get("copy_from","").strip()
        if not new_map:
            return jsonify({"status":"error","message":"new_map_code is required"}),400

        db_view_util.create_map_code(new_map, copy_from, user_id=1)
        return jsonify({"status":"success"}),200
    except SQLAlchemyError as e:
        logger.exception("create_map_code error:")
        return jsonify({"status":"error","message":str(e)}),500


@account_tax_bp.route("/delete_mapping", methods=["POST"])
def delete_mapping():
    """
    JSON: { map_name: "SomeMap" }
    Removes the BudgetMap and all its accounts from DB.
    """
    try:
        body=request.get_json()
        map_name=body.get("map_name","").strip()
        if not map_name:
            return jsonify({"status":"error","message":"map_name is required"}),400
        db_view_util.delete_mapping(map_name)
        return jsonify({"status":"success"}),200
    except ValueError as ve:
        logger.warning("delete_mapping ValueError: %s",ve)
        return jsonify({"status":"error","message":str(ve)}),400
    except IntegrityError:
        logger.exception("delete_mapping IntegrityError:")
        return jsonify({"status":"error","message":"Database integrity error."}),500
    except SQLAlchemyError as e:
        logger.exception("delete_mapping error:")
        return jsonify({"status":"error","message":str(e)}),500


@account_tax_bp.route("/add_ledger", methods=["POST"])
def add_ledger():
    """
    JSON: {
      "map_name": "SomeMap",
      "ledger_name": "Ledger2025"
      "src_ledger": "12"   <-- ID of currently selected ledger in the dropdown
    }
    We copy tax codes from 'src_ledger' if provided, otherwise single placeholder.
    """
    try:
        body=request.get_json()
        map_name=body.get("map_name","").strip()
        ledger_name=body.get("ledger_name","").strip()
        src_ledger=body.get("src_ledger","").strip()  # ID
        if not ledger_name:
            return jsonify({"status":"error","message":"ledger_name required"}),400

        new_id, actual_name = db_view_util.add_ledger_custom(
            current_map=map_name,
            ledger_name=ledger_name,
            user_id=1,
            src_ledger=src_ledger
        )
        return jsonify({"status":"success","ledger_id":new_id,"actual_ledger_name":actual_name}),200
    except SQLAlchemyError as e:
        logger.exception("add_ledger error:")
        return jsonify({"status":"error","message":str(e)}),500


@account_tax_bp.route("/rename_ledger", methods=["POST"])
def rename_ledger():
    """
    JSON: { "old_name": "Ledger2024", "new_name": "Ledger2025" }
    """
    try:
        body=request.get_json()
        old_name=body.get("old_name","").strip()
        new_name=body.get("new_name","").strip()
        if not old_name or not new_name:
            return jsonify({"status":"error","message":"Both old_name & new_name are required"}),400

        db_view_util.rename_ledger(old_name, new_name)
        return jsonify({"status":"success"}),200
    except ValueError as ve:
        logger.warning("rename_ledger ValueError: %s",ve)
        return jsonify({"status":"error","message":str(ve)}),400
    except SQLAlchemyError as e:
        logger.exception("rename_ledger error:")
        return jsonify({"status":"error","message":str(e)}),500


@account_tax_bp.route("/remove_ledger", methods=["POST"])
def remove_ledger():
    """
    JSON: { "map_name":"SomeMap", "ledger_name":"Ledger2025" }
    Clears references from that map's accounts & deletes the ledger parent item + tax codes.
    """
    try:
        body=request.get_json()
        map_name=body.get("map_name","").strip()
        ledger_name=body.get("ledger_name","").strip()
        if not map_name or not ledger_name:
            return jsonify({"status":"error","message":"map_name & ledger_name required"}),400

        db_view_util.delete_ledger(map_name, ledger_name)
        return jsonify({"status":"success"}),200
    except ValueError as ve:
        logger.warning("remove_ledger ValueError: %s",ve)
        return jsonify({"status":"error","message":str(ve)}),400
    except SQLAlchemyError as e:
        logger.exception("remove_ledger error:")
        return jsonify({"status":"error","message":str(e)}),500


@account_tax_bp.route("/create_tax_code", methods=["POST"])
def create_tax_code():
    """
    JSON: { "ledger_id": "12", "tax_code": "5300", "description": "Tax desc" }
    Creates a new TaxAccount row referencing that ledger_id.
    """
    try:
        body=request.get_json()
        led_id=body.get("ledger_id","").strip()
        code=body.get("tax_code","").strip()
        desc=body.get("description","").strip()
        if not led_id or not code:
            return jsonify({"status":"error","message":"ledger_id & tax_code required"}),400

        new_id=db_view_util.create_tax_record(code, desc, int(led_id))
        return jsonify({"status":"success","tax_id":new_id}),200
    except SQLAlchemyError as e:
        logger.exception("create_tax_code error:")
        return jsonify({"status":"error","message":str(e)}),500


@account_tax_bp.route("/update_tax", methods=["POST"])
def update_tax():
    """
    JSON: { "map_name":"SomeMap", "tax_id":123, "tax_code":"5300", "tax_description":"New" }
    """
    try:
        body=request.get_json()
        tax_id=body.get("tax_id",None)
        tax_code=body.get("tax_code","").strip()
        tax_desc=body.get("tax_description","").strip()
        if not tax_id or not tax_code:
            return jsonify({"status":"error","message":"tax_id & tax_code required"}),400

        db_view_util.update_tax_record(int(tax_id), tax_code, tax_desc, None)
        return jsonify({"status":"success"}),200
    except ValueError as ve:
        logger.warning("update_tax ValueError: %s",ve)
        return jsonify({"status":"error","message":str(ve)}),400
    except SQLAlchemyError as e:
        logger.exception("update_tax error:")
        return jsonify({"status":"error","message":str(e)}),500


@account_tax_bp.route("/delete_tax", methods=["POST"])
def delete_tax():
    """
    JSON: { "map_name":"SomeMap", "tax_id":123 }
    Clears references from accounts in that map, then deletes the tax row.
    """
    try:
        body=request.get_json()
        map_name=body.get("map_name","").strip()
        tax_id=body.get("tax_id",None)
        if not map_name or not tax_id:
            return jsonify({"status":"error","message":"map_name & tax_id required"}),400

        db_view_util.delete_tax_record(map_name, int(tax_id))
        return jsonify({"status":"success"}),200
    except ValueError as ve:
        logger.warning("delete_tax ValueError: %s",ve)
        return jsonify({"status":"error","message":str(ve)}),400
    except SQLAlchemyError as e:
        logger.exception("delete_tax error:")
        return jsonify({"status":"error","message":str(e)}),500


@account_tax_bp.route("/assign_tax_bulk", methods=["POST"])
def assign_tax_bulk():
    """
    JSON: {
      "map_name":"SomeMap",
      "account_ids":[1,2,3],
      "tax_id":999
    }
    """
    try:
        data=request.get_json()
        map_name=data.get("map_name","").strip()
        account_ids=data.get("account_ids",[])
        tax_id=data.get("tax_id",None)
        if not map_name or not account_ids or not tax_id:
            return jsonify({"status":"error","message":"map_name,account_ids,tax_id required"}),400

        db_view_util.assign_tax_bulk(map_name, account_ids, int(tax_id))
        return jsonify({"status":"success"}),200
    except ValueError as ve:
        logger.warning("assign_tax_bulk ValueError: %s",ve)
        return jsonify({"status":"error","message":str(ve)}),400
    except IntegrityError:
        logger.exception("assign_tax_bulk IntegrityError:")
        return jsonify({"status":"error","message":"Database integrity error."}),500
    except SQLAlchemyError as e:
        logger.exception("assign_tax_bulk error:")
        return jsonify({"status":"error","message":str(e)}),500