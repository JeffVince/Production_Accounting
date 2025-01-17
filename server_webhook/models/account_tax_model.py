import logging
import re
from typing import List, Dict, Any, Optional
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from database.database_util import DatabaseOperations
from models import AccountCode, TaxAccount, BudgetMap, TaxLedger
from database.db_util import get_db_session

logger = logging.getLogger(__name__)

def natural_sort_key(s: str) -> List:
    """
    Splits the string for natural numeric sorting: '2' < '10', etc.
    '10000-4' => [10000,'-',4]
    """
    s = s or ""
    return [int(part) if part.isdigit() else part.lower()
            for part in re.split(r'(\d+)', s)]

class AccountTaxModel:
    def __init__(self):
        self.db_ops = DatabaseOperations()

    def fetch_all_map_names(self) -> List[str]:
        with get_db_session() as session:
            rows=session.query(BudgetMap).all()
            return sorted([bm.map_name for bm in rows])

    def fetch_map_data(self,
                       map_name:str,
                       page_account:int=1,
                       per_page_account:int=40,
                       ledger_id:str="",
                       sort_by:str="code_natural",
                       direction:str="asc") -> Dict[str,Any]:
        data={
            "account_records":[],
            "tax_records":[],
            "page_account":page_account,
            "total_pages_account":1
        }
        with get_db_session() as session:
            bm=session.query(BudgetMap).filter(BudgetMap.map_name==map_name).one_or_none()
            if not bm or not ledger_id:
                return data

            lid=int(ledger_id)
            taxRows=session.query(TaxAccount).filter(TaxAccount.tax_ledger_id==lid).all()
            tax_ids=[t.id for t in taxRows]

            query=(
                session.query(
                    AccountCode.id,
                    AccountCode.code,
                    AccountCode.account_description,
                    AccountCode.tax_id,
                    AccountCode.updated_at,
                    TaxAccount.tax_code
                )
                .outerjoin(TaxAccount, AccountCode.tax_id==TaxAccount.id)
                .filter(AccountCode.budget_map_id==bm.id)
                .filter(AccountCode.tax_id.in_(tax_ids))
            )

            allRows = query.all()
            # handle sorting
            if sort_by=="code_natural":
                # python-level natural sort
                allRows=sorted(allRows, key=lambda r: natural_sort_key(r.code or ""))
                if direction=="desc":
                    allRows.reverse()
            elif sort_by=="code":
                if direction=="desc":
                    allRows=sorted(allRows, key=lambda r: (r.code or ""), reverse=True)
                else:
                    allRows=sorted(allRows, key=lambda r: (r.code or ""))
            elif sort_by=="description":
                if direction=="desc":
                    allRows=sorted(allRows, key=lambda r: (r.account_description or "").lower(), reverse=True)
                else:
                    allRows=sorted(allRows, key=lambda r: (r.account_description or "").lower())
            elif sort_by=="linked_tax":
                if direction=="desc":
                    allRows=sorted(allRows, key=lambda r: (r.tax_code or "").lower(), reverse=True)
                else:
                    allRows=sorted(allRows, key=lambda r: (r.tax_code or "").lower())
            elif sort_by=="updated":
                if direction=="desc":
                    allRows=sorted(allRows, key=lambda r: (r.updated_at or ""), reverse=True)
                else:
                    allRows=sorted(allRows, key=lambda r: (r.updated_at or ""))

            total = len(allRows)
            total_pages = (total//per_page_account) + (1 if total%per_page_account!=0 else 0)
            start=(page_account-1)*per_page_account
            end=start+per_page_account
            selected=allRows[start:end]

            account_records=[]
            for row in selected:
                account_records.append({
                    "id": row.id,
                    "code": row.code,
                    "account_description": row.account_description,
                    "tax_id": row.tax_id,
                    "tax_code": row.tax_code,
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None
                })

            data["account_records"]=account_records
            data["page_account"]=page_account
            data["total_pages_account"]=total_pages

            tax_records=[]
            for t in taxRows:
                tax_records.append({
                    "id": t.id,
                    "tax_code": t.tax_code,
                    "description": t.description
                })
            data["tax_records"]=tax_records
        return data

    def create_map_code(self, new_map_name:str, copy_from_name:str="", user_id:int=1)->None:
        with get_db_session() as session:
            newBM=BudgetMap(map_name=new_map_name, user_id=user_id)
            session.add(newBM)
            session.commit()
            if copy_from_name:
                oldBM=session.query(BudgetMap).filter(BudgetMap.map_name==copy_from_name).one_or_none()
                if oldBM:
                    oldAccts=session.query(AccountCode).filter(AccountCode.budget_map_id==oldBM.id).all()
                    for oa in oldAccts:
                        newA=AccountCode(
                            code=oa.code,
                            account_description=oa.account_description,
                            tax_id=oa.tax_id,
                            budget_map_id=newBM.id
                        )
                        session.add(newA)
            session.commit()

    def delete_mapping(self, map_name:str)->None:
        with get_db_session() as session:
            bm=session.query(BudgetMap).filter(BudgetMap.map_name==map_name).one_or_none()
            if not bm:
                raise ValueError(f"No BudgetMap found for name='{map_name}'")
            accts=session.query(AccountCode).filter(AccountCode.budget_map_id==bm.id).all()
            if not accts:
                raise ValueError(f"No accounts found for map_name='{map_name}'")
            for a in accts:
                session.delete(a)
            # remove the parent BudgetMap
            session.delete(bm)
            session.commit()

    def add_ledger_custom(self,
                          current_map:str,
                          ledger_name:str,
                          user_id:int=1,
                          src_ledger:str="",
                          src_map:str="") -> (int,str):
        """
        Creates a ledger with ledger_name. If src_ledger is provided => copy those tax codes.
        """
        with get_db_session() as session:
            newL=TaxLedger(name=ledger_name, user_id=user_id)
            session.add(newL)
            session.flush()

            if src_ledger:
                try:
                    old_ledger_id=int(src_ledger)
                    oldLed=session.query(TaxLedger).get(old_ledger_id)
                    if oldLed:
                        oldTaxes=session.query(TaxAccount).filter(TaxAccount.tax_ledger_id==oldLed.id).all()
                        for ot in oldTaxes:
                            newTax=TaxAccount(
                                tax_code=ot.tax_code,
                                description=ot.description,
                                tax_ledger_id=newL.id
                            )
                            session.add(newTax)
                except ValueError:
                    pass
            else:
                # single placeholder
                placeholder=TaxAccount(
                    tax_code="PLACEHOLDER",
                    description="Placeholder Code",
                    tax_ledger_id=newL.id
                )
                session.add(placeholder)
            session.commit()
            return (newL.id, ledger_name)

    def rename_ledger(self, old_name:str, new_name:str)->None:
        with get_db_session() as session:
            row=session.query(TaxLedger).filter(TaxLedger.name==old_name).one_or_none()
            if not row:
                raise ValueError(f"Ledger '{old_name}' not found.")
            row.name=new_name
            session.commit()

    def delete_ledger(self, map_name:str, ledger_name:str)->None:
        """
        Delete the ledger row + any associated tax codes,
        clearing references from accounts in that map using them.
        """
        with get_db_session() as session:
            ld=session.query(TaxLedger).filter(TaxLedger.name==ledger_name).one_or_none()
            if not ld:
                raise ValueError(f"Ledger '{ledger_name}' not found.")
            # gather tax codes
            taxRows=session.query(TaxAccount).filter(TaxAccount.tax_ledger_id==ld.id).all()
            bm=session.query(BudgetMap).filter(BudgetMap.map_name==map_name).one_or_none()
            if bm:
                for tx in taxRows:
                    session.query(AccountCode).filter(
                        AccountCode.budget_map_id==bm.id,
                        AccountCode.tax_id==tx.id
                    ).update({AccountCode.tax_id:None}, synchronize_session=False)
                    session.delete(tx)
            session.delete(ld)
            session.commit()

    def create_tax_record(self, tax_code:str, tax_desc:str, tax_ledger_id:int)->int:
        with get_db_session() as session:
            tax=TaxAccount(tax_code=tax_code, description=tax_desc, tax_ledger_id=tax_ledger_id)
            session.add(tax)
            session.commit()
            return tax.id

    def update_tax_record(self, tax_id:int, tax_code:str, tax_desc:str, tax_ledger_id:Optional[int])->None:
        with get_db_session() as session:
            row=session.query(TaxAccount).filter(TaxAccount.id==tax_id).one_or_none()
            if not row:
                raise ValueError(f"TaxAccount id={tax_id} not found.")
            row.tax_code=tax_code
            row.description=tax_desc
            if tax_ledger_id:
                row.tax_ledger_id=tax_ledger_id
            session.commit()

    def delete_tax_record(self, map_name:str, tax_id:int)->None:
        with get_db_session() as session:
            tax=session.query(TaxAccount).get(tax_id)
            if not tax:
                raise ValueError(f"Tax ID={tax_id} not found.")
            bm=session.query(BudgetMap).filter(BudgetMap.map_name==map_name).one_or_none()
            if not bm:
                raise ValueError(f"No BudgetMap found for '{map_name}'")

            # Clear references from accounts in that map
            session.query(AccountCode).filter(
                AccountCode.budget_map_id==bm.id,
                AccountCode.tax_id==tax_id
            ).update({AccountCode.tax_id:None}, synchronize_session=False)
            session.delete(tax)
            session.commit()

    def assign_tax_bulk(self, map_name:str, account_ids:List[int], tax_id:int)->None:
        with get_db_session() as session:
            bm=session.query(BudgetMap).filter(BudgetMap.map_name==map_name).one_or_none()
            if not bm:
                raise ValueError(f"Map '{map_name}' does not exist.")
            tax=session.query(TaxAccount).filter(TaxAccount.id==tax_id).one_or_none()
            if not tax:
                raise ValueError(f"Tax ID={tax_id} not found.")
            updated=session.query(AccountCode).filter(
                AccountCode.budget_map_id==bm.id,
                AccountCode.id.in_(account_ids)
            ).update({AccountCode.tax_id:tax_id}, synchronize_session=False)
            session.commit()
            if updated==0:
                raise ValueError("No accounts were updated. Check account IDs or map name.")