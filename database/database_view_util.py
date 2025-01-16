import re

from sqlalchemy import or_, func
from sqlalchemy.exc import IntegrityError
from database.database_util import DatabaseOperations
from models import AccountCode, TaxAccount, BudgetMap
from typing import List, Dict, Any
from database.db_util import get_db_session  # If needed for custom queries

class DatabaseViewUtil:
    """
    Utility class for HTML-based view operations,
    e.g., custom joins, bulk updates, or any logic specific
    to how data is displayed/edited in templates.
    """

    def __init__(self):
        # We can use the standard DatabaseOperations class for base CRUD
        self.db_ops = DatabaseOperations()

        # -------------------------------------------------------------------
        # 1) FETCH ALL MAP NAMES
        # -------------------------------------------------------------------

    def fetch_all_map_names(self) -> List[str]:
        """
        Return a list of all BudgetMap.map_name values, sorted alphabetically.
        """
        with get_db_session() as session:
            rows = session.query(BudgetMap).all()
            names = [bm.map_name for bm in rows]
            return sorted(names)

        # -------------------------------------------------------------------
        # 2) FETCH DATA FOR A SPECIFIC MAP (Accounts & Tax)
        # -------------------------------------------------------------------

    def fetch_map_data(
            self,
            map_name: str,
            page_account: int = 1,
            per_page_account: int = 40,
            page_tax: int = 1,  # Not used in this example, but left for consistency
            per_page_tax: int = 10,
            sort_by: str = None
    ) -> Dict[str, Any]:
        """
        Returns paginated account_records + tax_records for a given BudgetMap.map_name,
        plus pagination info. Supports sorting by:
          - 'code' (simple lexicographical)
          - 'description'
          - 'code_natural' (Python-based “natural” sort)
          - 'linked_tax'
          - 'updated'
        """
        data = {
            "account_records": [],
            "tax_records": [],
            "page_account": page_account,
            "page_tax": page_tax,
            "total_pages_account": 1,
            "total_pages_tax": 1
        }

        with get_db_session() as session:
            # 1) Locate the BudgetMap for this map_name
            budget_map = session.query(BudgetMap).filter(BudgetMap.map_name == map_name).one_or_none()
            if not budget_map:
                # If not found, just return empty data or raise an exception
                return data

            # 2) Query for AccountCode rows that belong to that BudgetMap
            acct_query = (
                session.query(
                    AccountCode.id,
                    AccountCode.code,
                    AccountCode.account_description,
                    AccountCode.tax_id,
                    AccountCode.updated_at,
                    TaxAccount.tax_code
                )
                    .outerjoin(TaxAccount, AccountCode.tax_id == TaxAccount.id)
                    .filter(AccountCode.budget_map_id == budget_map.id)
            )

            # Apply sorting
            if sort_by == "code":
                acct_query = acct_query.order_by(AccountCode.code)
            elif sort_by == "description":
                acct_query = acct_query.order_by(AccountCode.account_description)
            elif sort_by == "linked_tax":
                acct_query = acct_query.order_by(TaxAccount.tax_code)
            elif sort_by == "updated":
                acct_query = acct_query.order_by(AccountCode.updated_at.desc())
            elif sort_by == "code_natural":
                # We'll do Python-level sorting after fetching
                pass

            acct_rows = acct_query.all()

            # If 'code_natural', then we do that locally
            if sort_by == "code_natural":
                acct_rows = sorted(acct_rows, key=lambda row: natural_sort_key(row.code))

            # Pagination
            total_accounts = len(acct_rows)
            total_pages_account = (total_accounts // per_page_account) + (
                1 if total_accounts % per_page_account != 0 else 0
            )
            offset_account = (page_account - 1) * per_page_account
            selected_acct_rows = acct_rows[offset_account: offset_account + per_page_account]

            # Convert to dictionary
            account_records = []
            for row in selected_acct_rows:
                account_records.append({
                    "id": row.id,
                    "code": row.code,
                    "account_description": row.account_description,
                    "tax_id": row.tax_id,
                    "tax_code": row.tax_code,
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None
                })

            data["account_records"] = account_records
            data["page_account"] = page_account
            data["total_pages_account"] = total_pages_account

            # 3) Tax records: we simply fetch all existing TaxAccount rows
            tax_rows = session.query(TaxAccount).all()
            tax_records = []
            for t in tax_rows:
                tax_records.append({
                    "id": t.id,
                    "tax_code": t.tax_code,
                    "description": t.description
                })
            data["tax_records"] = tax_records
            data["page_tax"] = 1
            data["total_pages_tax"] = 1

        return data

        # -------------------------------------------------------------------
        # 3) CREATE / DELETE MAPS
        # -------------------------------------------------------------------

    def create_map_code(self, new_map_name: str, copy_from_name: str = "") -> None:
        """
        Insert a new BudgetMap row with map_name=new_map_name.
        If copy_from_name is provided, duplicate the AccountCode rows from that map.
        """
        with get_db_session() as session:
            # 1) Create the new BudgetMap
            new_bm = BudgetMap(map_name=new_map_name, user_id=1)  # user_id=1 is placeholder
            session.add(new_bm)
            session.commit()

            # 2) If copy_from_name is set, find that old map
            if copy_from_name:
                old_bm = session.query(BudgetMap).filter(BudgetMap.map_name == copy_from_name).one_or_none()
                if old_bm:
                    old_accts = session.query(AccountCode).filter(AccountCode.budget_map_id == old_bm.id).all()
                    for old_acct in old_accts:
                        new_acct = AccountCode(
                            code=old_acct.code,
                            account_description=old_acct.account_description,
                            tax_id=old_acct.tax_id,
                            budget_map_id=new_bm.id
                        )
                        session.add(new_acct)
            session.commit()

    def delete_mapping(self, map_name: str) -> None:
        """
        Deletes all AccountCode rows associated with the given map_name.
        Optionally you could remove the BudgetMap row itself.
        """
        with get_db_session() as session:
            bm = session.query(BudgetMap).filter(BudgetMap.map_name == map_name).one_or_none()
            if not bm:
                raise ValueError(f"No BudgetMap found for name='{map_name}'")

            acct_rows = session.query(AccountCode).filter(AccountCode.budget_map_id == bm.id).all()
            if not acct_rows:
                raise ValueError(f"No accounts found for map_name='{map_name}'")

            for row in acct_rows:
                session.delete(row)

            # If you want to remove the BudgetMap row as well, do:
            # session.delete(bm)

            session.commit()

        # -------------------------------------------------------------------
        # 4) TAX RECORD CRUD
        # -------------------------------------------------------------------

    def create_tax_record(self, tax_code: str, tax_desc: str) -> int:
        """
        Create a new TaxAccount row. (No map_name needed to store in DB.)
        """
        with get_db_session() as session:
            tax = TaxAccount(
                tax_code=tax_code,
                description=tax_desc
            )
            session.add(tax)
            session.commit()
            return tax.id

    def delete_tax_record(self, map_name: str, tax_id: int) -> None:
        """
        Delete the tax row by ID. Also clear references from accounts in the given map_name.
        """
        with get_db_session() as session:
            # 1) Find the BudgetMap
            bm = session.query(BudgetMap).filter(BudgetMap.map_name == map_name).one_or_none()
            if not bm:
                raise ValueError(f"No BudgetMap found for name='{map_name}'")

            tax_row = session.query(TaxAccount).get(tax_id)
            if not tax_row:
                raise ValueError(f"Tax ID {tax_id} not found.")

            # Clear references from accounts in that BudgetMap
            session.query(AccountCode).filter(
                AccountCode.tax_id == tax_id,
                AccountCode.budget_map_id == bm.id
            ).update({AccountCode.tax_id: None}, synchronize_session=False)

            session.delete(tax_row)
            session.commit()

    def update_tax_record(self, tax_id: int, tax_code: str, tax_desc: str) -> None:
        """
        Update the tax record with new code & description.
        (If you want map_name logic for security, you can add that as well.)
        """
        with get_db_session() as session:
            tax_row = session.query(TaxAccount).filter(TaxAccount.id == tax_id).one_or_none()
            if not tax_row:
                raise ValueError(f"TaxAccount id={tax_id} not found.")
            tax_row.tax_code = tax_code
            tax_row.description = tax_desc
            session.commit()

        # -------------------------------------------------------------------
        # 5) ACCOUNT <-> TAX RELATIONSHIPS
        # -------------------------------------------------------------------

    def assign_tax_to_account(self, map_name: str, account_id: int, tax_id: int) -> None:
        """
        Update the given AccountCode with the new tax_id, ensuring it belongs to the correct map.
        """
        with get_db_session() as session:
            bm = session.query(BudgetMap).filter(BudgetMap.map_name == map_name).one_or_none()
            if not bm:
                raise ValueError(f"No BudgetMap found for '{map_name}'")

            acct = session.query(AccountCode).filter(
                AccountCode.id == account_id,
                AccountCode.budget_map_id == bm.id
            ).one_or_none()
            if not acct:
                raise ValueError(f"AccountCode id={account_id} not found under map_name='{map_name}'")

            acct.tax_id = tax_id
            session.commit()

    def assign_tax_bulk(self, map_name: str, account_ids: List[int], tax_id: int) -> None:
        """
        Assigns a single tax_id to multiple account codes under a specific BudgetMap.
        """
        with get_db_session() as session:
            try:
                # Validate that the map_name exists
                bm = session.query(BudgetMap).filter(BudgetMap.map_name == map_name).one_or_none()
                if not bm:
                    raise ValueError(f"Map name '{map_name}' does not exist.")

                # Validate that tax_id exists
                tax_exists = session.query(TaxAccount).filter(TaxAccount.id == tax_id).one_or_none()
                if not tax_exists:
                    raise ValueError(f"Tax ID '{tax_id}' does not exist.")

                updated_rows = session.query(AccountCode).filter(
                    AccountCode.budget_map_id == bm.id,
                    AccountCode.id.in_(account_ids)
                ).update({AccountCode.tax_id: tax_id}, synchronize_session=False)

                session.commit()

                if updated_rows == 0:
                    raise ValueError("No accounts were updated. Check account IDs/map name.")
            except IntegrityError as e:
                session.rollback()
                raise IntegrityError(f"Database integrity error: {str(e)}") from e
            except Exception as e:
                session.rollback()
                raise e

    def get_all_account_with_tax(self, sort_by: str = None) -> List[Dict[str, Any]]:
        """
        Return a list of dicts, each containing:
          - accountCode fields: id, code, account_description, updated_at
          - TaxAccount fields: id, tax_code, description
        Possibly joined by accountCode.tax_id == TaxAccount.id
        with optional sorting by "code", "tax_code", or "updated".
        """
        with get_db_session() as session:
            # Base query: join accountCode -> TaxAccount
            query = (
                session.query(
                    AccountCode.id,
                    AccountCode.code,
                    AccountCode.account_description,
                    AccountCode.updated_at,
                    TaxAccount.id.label("tax_id"),
                    TaxAccount.tax_code,
                    TaxAccount.description
                )
                .outerjoin(TaxAccount, AccountCode.tax_id == TaxAccount.id)
            )

            # Optional sorting
            if sort_by == "code":
                # Example: sort by code while ignoring dashes
                query = query.order_by(AccountCode.code.replace("-",""))
            elif sort_by == "tax_code":
                query = query.order_by(TaxAccount.tax_code)
            elif sort_by == "updated":
                # Sort by most recently updated first
                query = query.order_by(AccountCode.updated_at.desc())

            result_rows = query.all()

        # Convert to list of dicts
        output = []
        for row in result_rows:
            output.append({
                "id": row.id,
                "code": row.code,
                "account_description": row.account_description,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                "tax_id": row.tax_id,
                "tax_code": row.tax_code,
                "description": row.description
            })
        return output

    def bulk_update_account_tax(self, updates_list: List[Dict[str, Any]]) -> None:
        """
        Accepts a list of dicts, each with:
          {
            "account_id": <int>,
            "code": <str>,
            "account_description": <str>,
            "tax_id": <int>,
            "tax_code": <str>,
            "tax_description": <str>
          }
        and updates both the accountCode + TaxAccount entries.
        """
        with get_db_session() as session:
            for row_data in updates_list:
                # 1) Update the Account Code
                account_row = session.query(AccountCode).get(row_data["id"])
                if account_row:
                    account_row.code = row_data["code"]
                    account_row.account_description = row_data["account_description"]

                    # 2) Update the linked TaxAccount
                    if row_data["tax_id"]:
                        tax_row = session.query(TaxAccount).get(row_data["tax_id"])
                        if tax_row:
                            tax_row.tax_code = row_data["tax_code"]
                            tax_row.description = row_data["description"]

            session.commit()

    def fetch_all_map_codes(self) -> List[str]:
        """
        Return a list of all distinct map_code values from AccountCode.
        """
        with get_db_session() as session:
            rows = session.query(AccountCode.map_if).distinct().all()
            map_codes = [r[0] for r in rows if r[0]]
            return sorted(map_codes)

    def fetch_map_code_data(
            self,
            map_code: str,
            page_account: int = 1,
            per_page_account: int = 40,
            page_tax: int = 1,
            per_page_tax: int = 10,
            sort_by: str = None
    ) -> Dict[str, Any]:
        """
        Returns paginated account_records + tax_records for a given map_code,
        plus pagination info. Supports sorting by:
          - 'code' (simple lexicographical)
          - 'description'
          - 'code_natural' (Python-based “natural” sort)
          - 'linked_tax'
          - 'updated'
        """
        data = {
            "account_records": [],
            "tax_records": [],
            "page_account": page_account,
            "page_tax": page_tax,
            "total_pages_account": 1,
            "total_pages_tax": 1
        }

        with get_db_session() as session:
            # 1) Base query for accounts
            acct_query = (
                session.query(
                    AccountCode.id,
                    AccountCode.code,
                    AccountCode.account_description,
                    AccountCode.tax_id,
                    AccountCode.updated_at,
                    TaxAccount.tax_code
                )
                .outerjoin(TaxAccount, AccountCode.tax_id == TaxAccount.id)
                .filter(AccountCode.map_code == map_code)
            )

            # Sorting possibilities:
            if sort_by == "code":
                acct_query = acct_query.order_by(AccountCode.code)
            elif sort_by == "description":
                acct_query = acct_query.order_by(AccountCode.account_description)
            elif sort_by == "linked_tax":
                acct_query = acct_query.order_by(TaxAccount.tax_code)
            elif sort_by == "updated":
                acct_query = acct_query.order_by(AccountCode.updated_at.desc())
            elif sort_by == "modified_asc":
                acct_query = acct_query.order_by(AccountCode.modified_at.asc())  # If you track modified_at
            elif sort_by == "modified_desc":
                acct_query = acct_query.order_by(AccountCode.modified_at.desc())

            acct_rows = acct_query.all()

            # code_natural => Python-based sorting on code
            if sort_by == "code_natural":
                acct_rows = sorted(
                    acct_rows,
                    key=lambda row: natural_sort_key(row.code)
                )

            # 4) Paginate
            total_accounts = len(acct_rows)
            total_pages_account = (total_accounts // per_page_account) + (
                1 if total_accounts % per_page_account != 0 else 0
            )
            offset_account = (page_account - 1) * per_page_account
            selected_acct_rows = acct_rows[offset_account: offset_account + per_page_account]

            account_records = []
            for row in selected_acct_rows:
                account_records.append({
                    "id": row.id,
                    "code": row.code,
                    "account_description": row.account_description,
                    "tax_id": row.tax_id,
                    "tax_code": row.tax_code,
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None
                })

            data["account_records"] = account_records
            data["page_account"] = page_account
            data["total_pages_account"] = total_pages_account

            # 5) Tax records
            tax_query = session.query(TaxAccount.id, TaxAccount.tax_code, TaxAccount.description)
            tax_rows = tax_query.all()
            tax_records = []
            for t in tax_rows:
                tax_records.append({
                    "id": t.id,
                    "tax_code": t.tax_code,
                    "description": t.description
                })

            data["tax_records"] = tax_records
            data["page_tax"] = 1
            data["total_pages_tax"] = 1

        return data




def natural_sort_key(s: str) -> list:
    """
    Splits the string into alphanumeric chunks so that
    '100-3' > '100' > '10' > '2' sorts properly as [100, '-', 3], etc.
    """
    s = s or ""
    return [int(part) if part.isdigit() else part.lower()
            for part in re.split(r'(\d+)', s)]