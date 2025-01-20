# ================================ 3) account_tax_model.py ================================
import logging
import re
from typing import List, Dict, Any, Optional
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from database.database_util import DatabaseOperations  # Adjust imports
from models import AccountCode, TaxAccount, BudgetMap, TaxLedger
from database.db_util import get_db_session

logger = logging.getLogger("admin_logger")

def natural_sort_key(s: str) -> List:
    s = s or ""
    return [
        int(part) if part.isdigit() else part.lower()
        for part in re.split(r'(\d+)', s)
    ]

class AccountTaxModel:
    def __init__(self):
        self.db_ops = DatabaseOperations()

    def fetch_all_map_names(self) -> List[str]:
        with get_db_session() as session:
            rows=session.query(BudgetMap).all()
            return sorted([bm.map_name for bm in rows])

    def fetch_map_data(
        self,
        map_name:str,
        page_account:int=1,
        per_page_account:int=40,
        ledger_id:str="",
        sort_by:str="code_natural",
        direction:str="asc"
    ) -> Dict[str,Any]:
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
            # fetch all tax codes for this ledger
            taxRows=session.query(TaxAccount).filter(TaxAccount.tax_ledger_id==lid).all()
            tax_ids=[t.id for t in taxRows]

            acct_query=(
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
            rows=acct_query.all()

            # Sorting
            if sort_by=="code_natural":
                rows=sorted(rows, key=lambda r: natural_sort_key(r.code or ""))
            elif sort_by=="code":
                rows=sorted(rows, key=lambda r: (r.code or "").lower())
            elif sort_by=="description":
                rows=sorted(rows, key=lambda r: (r.account_description or "").lower())
            elif sort_by=="linked_tax":
                rows=sorted(rows, key=lambda r: (r.tax_code or "").lower())
            elif sort_by=="updated":
                rows=sorted(rows, key=lambda r: (r.updated_at or ""))

            if direction=="desc":
                rows.reverse()

            total=len(rows)
            total_pages=(total//per_page_account)+(1 if total%per_page_account!=0 else 0)
            start=(page_account-1)*per_page_account
            end=start+per_page_account
            selected=rows[start:end]

            acct_records=[]
            for r in selected:
                acct_records.append({
                    "id": r.id,
                    "code": r.code,
                    "account_description": r.account_description,
                    "tax_id": r.tax_id,
                    "tax_code": r.tax_code,
                    "updated_at": r.updated_at.isoformat() if r.updated_at else None
                })
            data["account_records"]=acct_records
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
            session.delete(bm)
            session.commit()

    def fetch_ledgers_for_map(self, map_name:str)->List[Dict[str,Any]]:
        with get_db_session() as session:
            bm=session.query(BudgetMap).filter(BudgetMap.map_name==map_name).one_or_none()
            if not bm:
                return []
            q=(
                session.query(TaxLedger.id, TaxLedger.name, func.count(TaxAccount.id).label("cnt"))
                .join(TaxAccount, TaxLedger.id==TaxAccount.tax_ledger_id)
                .join(AccountCode, AccountCode.tax_id==TaxAccount.id)
                .filter(AccountCode.budget_map_id==bm.id)
                .group_by(TaxLedger.id, TaxLedger.name)
                .having(func.count(TaxAccount.id)>0)
            )
            rows=q.all()
            out=[]
            for r in rows:
                out.append({"id":r.id,"name":r.name})
            return out

    def add_ledger_custom(
        self,
        current_map:str,
        ledger_name:str,
        user_id:int=1,
        src_ledger:str=""
    )->(int,str):
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

    def create_accounts_for_new_ledger(self, map_name:str, ledger_id:int, src_ledger:str):
        with get_db_session() as session:
            bm=session.query(BudgetMap).filter(BudgetMap.map_name==map_name).one_or_none()
            if not bm:
                return
            # build a map from oldTaxCode => newTaxId
            newTaxes=session.query(TaxAccount).filter(TaxAccount.tax_ledger_id==ledger_id).all()
            taxMap={}
            for nt in newTaxes:
                taxMap[nt.tax_code]=nt.id

            if src_ledger:
                try:
                    old_ledger_id=int(src_ledger)
                    oldTaxes=session.query(TaxAccount).filter(TaxAccount.tax_ledger_id==old_ledger_id).all()
                    oldDict={o.tax_code:o.id for o in oldTaxes}

                    # find all accounts in that map which reference any old_ledger tax_id
                    oldAcctQuery=(
                        session.query(AccountCode)
                        .join(TaxAccount, AccountCode.tax_id==TaxAccount.id)
                        .filter(AccountCode.budget_map_id==bm.id)
                        .filter(TaxAccount.tax_ledger_id==old_ledger_id)
                    )
                    oldAccts=oldAcctQuery.all()
                    for oa in oldAccts:
                        oldTaxId=oa.tax_id
                        oldTaxRow=session.query(TaxAccount).get(oldTaxId)
                        if not oldTaxRow:
                            continue
                        newTaxId= taxMap.get(oldTaxRow.tax_code) or None
                        newAcct=AccountCode(
                            code=oa.code,
                            account_description=oa.account_description,
                            tax_id=newTaxId,
                            budget_map_id=bm.id
                        )
                        session.add(newAcct)
                    session.commit()
                except ValueError:
                    pass

    def rename_ledger(self, old_name:str, new_name:str)->None:
        with get_db_session() as session:
            row=session.query(TaxLedger).filter(TaxLedger.name==old_name).one_or_none()
            if not row:
                raise ValueError(f"Ledger '{old_name}' not found.")
            row.name=new_name
            session.commit()

    def delete_ledger(self, map_name:str, ledger_name:str)->None:
        with get_db_session() as session:
            ld=session.query(TaxLedger).filter(TaxLedger.name==ledger_name).one_or_none()
            if not ld:
                raise ValueError(f"Ledger '{ledger_name}' not found.")
            tRows=session.query(TaxAccount).filter(TaxAccount.tax_ledger_id==ld.id).all()
            bm=session.query(BudgetMap).filter(BudgetMap.map_name==map_name).one_or_none()
            if bm:
                for tx in tRows:
                    session.query(AccountCode).filter(
                        AccountCode.budget_map_id==bm.id,
                        AccountCode.tax_id==tx.id
                    ).update({AccountCode.tax_id:None}, synchronize_session=False)
                    session.delete(tx)
            session.delete(ld)
            session.commit()

    def create_tax_record(self, tax_code:str, tax_desc:str, tax_ledger_id:int)->int:
        with get_db_session() as session:
            t=TaxAccount(
                tax_code=tax_code,
                description=tax_desc,
                tax_ledger_id=tax_ledger_id
            )
            session.add(t)
            session.commit()
            return t.id

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
            t=session.query(TaxAccount).get(tax_id)
            if not t:
                raise ValueError(f"Tax ID={tax_id} not found.")
            bm=session.query(BudgetMap).filter(BudgetMap.map_name==map_name).one_or_none()
            if not bm:
                raise ValueError(f"No BudgetMap found for '{map_name}'")
            session.query(AccountCode).filter(
                AccountCode.budget_map_id==bm.id,
                AccountCode.tax_id==tax_id
            ).update({AccountCode.tax_id:None}, synchronize_session=False)
            session.delete(t)
            session.commit()

    def assign_tax_bulk(self, map_name:str, account_ids:List[int], tax_id:int)->None:
        with get_db_session() as session:
            bm=session.query(BudgetMap).filter(BudgetMap.map_name==map_name).one_or_none()
            if not bm:
                raise ValueError(f"Map '{map_name}' does not exist.")
            tx=session.query(TaxAccount).filter(TaxAccount.id==tax_id).one_or_none()
            if not tx:
                raise ValueError(f"Tax ID={tax_id} not found.")
            updated=session.query(AccountCode).filter(
                AccountCode.budget_map_id==bm.id,
                AccountCode.id.in_(account_ids)
            ).update({AccountCode.tax_id:tax_id}, synchronize_session=False)
            session.commit()
            if updated==0:
                raise ValueError("No accounts were updated. Check account IDs or map name.")

    def get_all_account_with_tax(self, sort_by):
        pass