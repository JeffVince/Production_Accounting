import logging
import re
from typing import List, Dict, Any, Optional
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from database.database_util import DatabaseOperations
from models import AccountCode, TaxAccount, BudgetMap, TaxLedger
from database.db_util import get_db_session

logger = logging.getLogger(__name__)

def natural_sort_key(s: str) -> list:
    s = s or ""
    return [int(part) if part.isdigit() else part.lower()
            for part in re.split(r'(\d+)', s)]

class DatabaseViewUtil:
    """
    Production-ready code with ledger + map management,
    including ASC/DESC toggles for sorting columns in fetch_map_data.
    """
    def __init__(self):
        self.db_ops = DatabaseOperations()

    def fetch_all_map_names(self) -> List[str]:
        try:
            with get_db_session() as session:
                rows=session.query(BudgetMap).all()
                return sorted(bm.map_name for bm in rows)
        except SQLAlchemyError as e:
            logger.exception("fetch_all_map_names error:")
            raise

    def fetch_map_data(self,
                       map_name:str,
                       page_account:int=1,
                       per_page_account:int=40,
                       ledger_id:str="",
                       sort_by:str="code_natural",
                       direction:str="asc") -> Dict[str,Any]:
        """
        direction='asc' or 'desc'
        sort_by in ['code','description','linked_tax','updated','code_natural']
        """
        data={
            "account_records":[],
            "tax_records":[],
            "page_account":page_account,
            "total_pages_account":1
        }
        try:
            with get_db_session() as session:
                bm=session.query(BudgetMap).filter(BudgetMap.map_name==map_name).one_or_none()
                if not bm:
                    return data
                if not ledger_id:
                    return data

                lid=int(ledger_id)
                taxRows=session.query(TaxAccount).filter(TaxAccount.tax_ledger_id==lid).all()
                taxIds=[t.id for t in taxRows]

                acctQuery=(
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
                    .filter(AccountCode.tax_id.in_(taxIds))
                )
                # handle sorting
                if sort_by=="code":
                    if direction=="asc":
                        acctQuery=acctQuery.order_by(AccountCode.code.asc())
                    else:
                        acctQuery=acctQuery.order_by(AccountCode.code.desc())
                elif sort_by=="description":
                    if direction=="asc":
                        acctQuery=acctQuery.order_by(AccountCode.account_description.asc())
                    else:
                        acctQuery=acctQuery.order_by(AccountCode.account_description.desc())
                elif sort_by=="linked_tax":
                    if direction=="asc":
                        acctQuery=acctQuery.order_by(TaxAccount.tax_code.asc())
                    else:
                        acctQuery=acctQuery.order_by(TaxAccount.tax_code.desc())
                elif sort_by=="updated":
                    if direction=="asc":
                        acctQuery=acctQuery.order_by(AccountCode.updated_at.asc())
                    else:
                        acctQuery=acctQuery.order_by(AccountCode.updated_at.desc())
                elif sort_by=="code_natural":
                    # We'll do python-level sorting after fetch
                    # then apply direction
                    rows=acctQuery.all()
                    rows=sorted(rows, key=lambda x: natural_sort_key(x.code or ""))
                    if direction=="desc":
                        rows.reverse()
                    # manual pagination
                    total=len(rows)
                    totalPages=(total//per_page_account)+(1 if total%per_page_account!=0 else 0)
                    start=(page_account-1)*per_page_account
                    end=start+per_page_account
                    selected=rows[start:end]
                    data["account_records"]=[]
                    for s in selected:
                        data["account_records"].append({
                            "id": s.id,
                            "code": s.code,
                            "account_description": s.account_description,
                            "tax_id": s.tax_id,
                            "tax_code": s.tax_code,
                            "updated_at": s.updated_at.isoformat() if s.updated_at else None
                        })
                    data["page_account"]=page_account
                    data["total_pages_account"]=totalPages
                    data["tax_records"]=[]
                    for t in taxRows:
                        data["tax_records"].append({
                            "id": t.id,
                            "tax_code": t.tax_code,
                            "description": t.description
                        })
                    return data
                # if we didn't return, we still have a query
                allAccts=acctQuery.all()
                total=len(allAccts)
                totalPages=(total//per_page_account)+(1 if total%per_page_account!=0 else 0)
                start=(page_account-1)*per_page_account
                end=start+per_page_account
                selected=allAccts[start:end]
                data["account_records"]=[]
                for s in selected:
                    data["account_records"].append({
                        "id": s.id,
                        "code": s.code,
                        "account_description": s.account_description,
                        "tax_id": s.tax_id,
                        "tax_code": s.tax_code,
                        "updated_at": s.updated_at.isoformat() if s.updated_at else None
                    })
                data["page_account"]=page_account
                data["total_pages_account"]=totalPages
                # tax
                data["tax_records"]=[]
                for t in taxRows:
                    data["tax_records"].append({
                        "id": t.id,
                        "tax_code": t.tax_code,
                        "description": t.description
                    })
                return data
        except SQLAlchemyError as e:
            logger.exception("fetch_map_data error:")
            raise

    def create_map_code(self, new_map_name:str, copy_from_name:str="", user_id:int=1)->None:
        try:
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
        except SQLAlchemyError as e:
            logger.exception("create_map_code error:")
            raise

    def delete_mapping(self, map_name:str)->None:
        try:
            with get_db_session() as session:
                bm=session.query(BudgetMap).filter(BudgetMap.map_name==map_name).one_or_none()
                if not bm:
                    raise ValueError(f"No BudgetMap found for name='{map_name}'")
                accts=session.query(AccountCode).filter(AccountCode.budget_map_id==bm.id).all()
                if not accts:
                    raise ValueError(f"No accounts found for map_name='{map_name}'")
                for a in accts:
                    session.delete(a)
                # remove the BudgetMap itself
                session.delete(bm)
                session.commit()
        except SQLAlchemyError as e:
            logger.exception("delete_mapping error:")
            raise

    def fetch_ledgers_for_map(self, map_name:str)->List[Dict[str,Any]]:
        try:
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
        except SQLAlchemyError as e:
            logger.exception("fetch_ledgers_for_map error:")
            raise

    def add_ledger_custom(self, current_map:str, ledger_name:str, user_id:int=1,
                          src_ledger:str="", src_map:str="")->(int,str):
        """
        Create a ledger with user-supplied name. No “-copy” suffix.
        If src_ledger is set, copy its tax codes.
        If src_map is set, copy that map's accounts => new ledger's first tax code.
        Return (ledger_id, ledger_name).
        """
        try:
            with get_db_session() as session:
                newLD=TaxLedger(name=ledger_name, user_id=user_id)
                session.add(newLD)
                session.flush()  # get newLD.id

                if src_ledger:
                    try:
                        lid=int(src_ledger)
                        oldLD=session.query(TaxLedger).get(lid)
                        if oldLD:
                            oldTaxes=session.query(TaxAccount).filter(TaxAccount.tax_ledger_id==lid).all()
                            for ot in oldTaxes:
                                newT=TaxAccount(tax_code=ot.tax_code, description=ot.description,
                                                tax_ledger_id=newLD.id)
                                session.add(newT)
                    except ValueError:
                        pass
                else:
                    # create single placeholder
                    place=TaxAccount(tax_code="PLACEHOLDER", description="Placeholder code",
                                     tax_ledger_id=newLD.id)
                    session.add(place)

                session.flush()

                if src_map:
                    oldMap=session.query(BudgetMap).filter(BudgetMap.map_name==src_map).one_or_none()
                    curMap=session.query(BudgetMap).filter(BudgetMap.map_name==current_map).one_or_none()
                    if oldMap and curMap:
                        oldAccts=session.query(AccountCode).filter(AccountCode.budget_map_id==oldMap.id).all()
                        newTaxes=session.query(TaxAccount).filter(TaxAccount.tax_ledger_id==newLD.id).all()
                        firstTid=None
                        if newTaxes:
                            firstTid=newTaxes[0].id
                        for oa in oldAccts:
                            newA=AccountCode(
                                code=oa.code,
                                account_description=oa.account_description,
                                tax_id=firstTid,
                                budget_map_id=curMap.id
                            )
                            session.add(newA)
                session.commit()
                return (newLD.id, ledger_name)
        except SQLAlchemyError as e:
            logger.exception("add_ledger_custom error:")
            raise

    def rename_ledger(self, old_name:str, new_name:str)->None:
        try:
            with get_db_session() as session:
                row=session.query(TaxLedger).filter(TaxLedger.name==old_name).one_or_none()
                if not row:
                    raise ValueError(f"Ledger '{old_name}' not found.")
                row.name=new_name
                session.commit()
        except SQLAlchemyError as e:
            logger.exception("rename_ledger error:")
            raise

    def delete_ledger(self, map_name:str, ledger_name:str)->None:
        """
        Also delete all tax codes referencing this ledger,
        and set relevant accounts' tax_id to None.
        """
        try:
            with get_db_session() as session:
                ld=session.query(TaxLedger).filter(TaxLedger.name==ledger_name).one_or_none()
                if not ld:
                    raise ValueError(f"Ledger '{ledger_name}' not found.")
                tRows=session.query(TaxAccount).filter(TaxAccount.tax_ledger_id==ld.id).all()
                bm=session.query(BudgetMap).filter(BudgetMap.map_name==map_name).one_or_none()
                if bm:
                    for trow in tRows:
                        session.query(AccountCode).filter(
                            AccountCode.budget_map_id==bm.id,
                            AccountCode.tax_id==trow.id
                        ).update({AccountCode.tax_id:None}, synchronize_session=False)
                        session.delete(trow)
                session.delete(ld)
                session.commit()
        except SQLAlchemyError as e:
            logger.exception("delete_ledger error:")
            raise

    def create_tax_record(self, code:str, desc:str, ledger_id:Optional[int])->int:
        try:
            with get_db_session() as session:
                t=TaxAccount(tax_code=code, description=desc, tax_ledger_id=ledger_id)
                session.add(t)
                session.commit()
                return t.id
        except SQLAlchemyError as e:
            logger.exception("create_tax_record error:")
            raise

    def update_tax_record(self, tax_id:int, tax_code:str, tax_desc:str, tax_ledger_id:Optional[int])->None:
        try:
            with get_db_session() as session:
                row=session.query(TaxAccount).filter(TaxAccount.id==tax_id).one_or_none()
                if not row:
                    raise ValueError(f"TaxAccount id={tax_id} not found.")
                row.tax_code=tax_code
                row.description=tax_desc
                if tax_ledger_id:
                    row.tax_ledger_id=tax_ledger_id
                session.commit()
        except SQLAlchemyError as e:
            logger.exception("update_tax_record error:")
            raise

    def delete_tax_record(self, map_name:str, tax_id:int)->None:
        try:
            with get_db_session() as session:
                row=session.query(TaxAccount).get(tax_id)
                if not row:
                    raise ValueError(f"Tax ID={tax_id} not found.")
                bm=session.query(BudgetMap).filter(BudgetMap.map_name==map_name).one_or_none()
                if not bm:
                    raise ValueError(f"No BudgetMap found for '{map_name}'")
                # Clear references
                session.query(AccountCode).filter(
                    AccountCode.budget_map_id==bm.id,
                    AccountCode.tax_id==tax_id
                ).update({AccountCode.tax_id:None}, synchronize_session=False)
                session.delete(row)
                session.commit()
        except SQLAlchemyError as e:
            logger.exception("delete_tax_record error:")
            raise

    def assign_tax_bulk(self, map_name:str, account_ids:List[int], tax_id:int)->None:
        try:
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
                    raise ValueError("No accounts updated. Check IDs or map name.")
        except SQLAlchemyError as e:
            logger.exception("assign_tax_bulk error:")
            raise