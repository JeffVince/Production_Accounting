#region 🚀 Imports
import logging
from sqlalchemy import (
    Column, String, DateTime, ForeignKey, UniqueConstraint, Index,
    text, Date, Integer, Numeric, BigInteger
)

from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

logging.getLogger('sqlalchemy.engine.Engine').setLevel(logging.ERROR)
logging.getLogger('sqlalchemy.pool').setLevel(logging.ERROR)

Base = declarative_base()
#endregion

#region 📄 Contact & User
class Contact(Base):
    __tablename__ = 'contact'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, index=True)
    vendor_status = Column(
        ENUM('PENDING', 'TO VERIFY', 'APPROVED', 'ISSUE', 'VERIFIED', name='vendor_status_enum'),
        nullable=False,
        server_default=text("'PENDING'::vendor_status_enum")
    )
    payment_details = Column(String(255), nullable=False, server_default='PENDING')
    vendor_type = Column(
        ENUM('VENDOR', 'CC', 'PC', 'INDIVIDUAL', 'S-CORP', 'C-CORP', name='vendor_type_enum'),
        nullable=True,
        server_default = text("'VENDOR'::vendor_type_enum")
    )
    email = Column(String(100), nullable=True)
    phone = Column(String(45), nullable=True)
    address_line_1 = Column(String(255), nullable=True)
    address_line_2 = Column(String(255), nullable=True)
    city = Column(String(100), nullable=True)
    zip = Column(String(20), nullable=True)
    region = Column(String(45), nullable=True)
    country = Column(String(100), nullable=True)
    tax_type = Column(String(45), nullable=True, server_default='SSN')
    tax_number = Column(String(45), nullable=True)
    pulse_id = Column(BigInteger, unique=True)
    xero_id = Column(String(255))
    tax_form_id = Column(BigInteger)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(
        DateTime,
        server_default=text('CURRENT_TIMESTAMP'),
        onupdate=text('CURRENT_TIMESTAMP')
    )

    purchase_orders = relationship(
        'PurchaseOrder',
        back_populates='contact',
        cascade='all, delete-orphan'
    )

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'vendor_status': self.vendor_status,
            'payment_details': self.payment_details,
            'vendor_type': self.vendor_type,
            'email': self.email,
            'phone': self.phone,
            'address_line_1': self.address_line_1,
            'address_line_2': self.address_line_2,
            'city': self.city,
            'zip': self.zip,
            'region': self.region,
            'country': self.country,
            'tax_type': self.tax_type,
            'tax_number': self.tax_number,
            'tax_form_id': self.tax_form_id,
            'pulse_id': self.pulse_id,
            'xero_id': self.xero_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }


class User(Base):
    __tablename__ = 'users'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    username = Column(String(100), nullable=False)
    contact_id = Column(
        BigInteger,
        ForeignKey('contact.id', onupdate='CASCADE', ondelete='SET NULL'),
        nullable=True
    )
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(
        DateTime,
        server_default=text('CURRENT_TIMESTAMP'),
        onupdate=text('CURRENT_TIMESTAMP')
    )

    def to_dict(self):
        return {
            'id': self.id,
            'username': self.username,
            'contact_id': self.contact_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
#endregion

#region 💲 BudgetMap & AccountCode (Parent/Child)
class BudgetMap(Base):
    __tablename__ = 'budget_map'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    map_name = Column(String(100), nullable=False)
    user_id = Column(
        BigInteger,
        ForeignKey('users.id', onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False
    )
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(
        DateTime,
        server_default=text('CURRENT_TIMESTAMP'),
        onupdate=text('CURRENT_TIMESTAMP')
    )

    def to_dict(self):
        return {
            'id': self.id,
            'map_name': self.map_name,
            'user_id': self.user_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }


class AccountCode(Base):
    __tablename__ = 'account_code'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    code = Column(String(45), nullable=False)
    budget_map_id = Column(
        BigInteger,
        ForeignKey('budget_map.id', onupdate='CASCADE', ondelete='SET NULL'),
        nullable=True
    )
    tax_id = Column(
        BigInteger,
        ForeignKey('tax_account.id', onupdate='CASCADE', ondelete='SET NULL'),
        nullable=True
    )
    account_description = Column(String(45), nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(
        DateTime,
        server_default=text('CURRENT_TIMESTAMP'),
        onupdate=text('CURRENT_TIMESTAMP')
    )

    tax_account = relationship('TaxAccount')

    def to_dict(self):
        return {
            'id': self.id,
            'code': self.code,
            'budget_map_id': self.budget_map_id,
            'tax_id': self.tax_id,
            'account_description': self.account_description,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
#endregion

#region 🧾 TaxLedger & TaxAccount (Parent/Child)
class TaxLedger(Base):
    __tablename__ = 'tax_ledger'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    user_id = Column(
        BigInteger,
        ForeignKey('users.id', onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False
    )
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(
        DateTime,
        server_default=text('CURRENT_TIMESTAMP'),
        onupdate=text('CURRENT_TIMESTAMP')
    )

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'user_id': self.user_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }


class TaxAccount(Base):
    __tablename__ = 'tax_account'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    tax_code = Column(String(45), nullable=False)
    description = Column(String(255), nullable=True)
    tax_ledger_id = Column(
        BigInteger,
        ForeignKey('tax_ledger.id', onupdate='CASCADE', ondelete='SET NULL'),
        nullable=True
    )
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(
        DateTime,
        server_default=text('CURRENT_TIMESTAMP'),
        onupdate=text('CURRENT_TIMESTAMP')
    )

    def to_dict(self):
        return {
            'id': self.id,
            'tax_code': self.tax_code,
            'description': self.description,
            'tax_ledger_id': self.tax_ledger_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
#endregion

#region 📜 AuditLog
class AuditLog(Base):
    __tablename__ = 'audit_log'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    table_id = Column(BigInteger, nullable=True)
    operation = Column(String(10), nullable=False)
    record_id = Column(BigInteger, nullable=True)
    message = Column(String(255), nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))

    def to_dict(self):
        return {
            'id': self.id,
            'table_id': self.table_id,
            'operation': self.operation,
            'record_id': self.record_id,
            'message': self.message,
            'created_at': self.created_at
        }
#endregion

#region 💰 BankTransaction
class BankTransaction(Base):
    __tablename__ = 'bank_transaction'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    mercury_transaction_id = Column(String(100), nullable=False, unique=True)
    state = Column(String(45), nullable=False, server_default='Pending')
    xero_bill_id = Column(
        BigInteger,
        ForeignKey('xero_bill.id', onupdate='NO ACTION', ondelete='NO ACTION'),
        nullable=False
    )
    xero_spend_money_id = Column(
        BigInteger,
        ForeignKey('spend_money.id', onupdate='NO ACTION', ondelete='NO ACTION'),
        nullable=False
    )
    tax_code = Column(Integer)
    amount = Column(Numeric)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'))

    xero_bill = relationship('XeroBill')
    spend_money = relationship('SpendMoney')

    def to_dict(self):
        return {
            'id': self.id,
            'mercury_transaction_id': self.mercury_transaction_id,
            'state': self.state,
            'tax_code': self.tax_code,
            'amount': self.amount,
            'xero_bill_id': self.xero_bill_id,
            'xero_spend_money_id': self.xero_spend_money_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
#endregion

#region 📑 XeroBill & XeroBillLineItem (Parent/Child)
class XeroBill(Base):
    __tablename__ = 'xero_bill'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    state = Column(String(45), nullable=False, server_default='Draft')
    project_number = Column(Integer, nullable=True)
    po_number = Column(Integer, nullable=True)
    detail_number = Column(Integer, nullable=True)
    transaction_date = Column(Date, nullable=True)
    due_date = Column(Date, nullable=True)
    contact_xero_id = Column(String(255), nullable=True)
    xero_reference_number = Column(String(50), nullable=True)

    xero_id = Column(String(255), nullable=True)
    xero_link = Column(String(255), nullable=True)
    updated_at = Column(
        DateTime,
        server_default=text('CURRENT_TIMESTAMP'),
        onupdate=text('CURRENT_TIMESTAMP')
    )
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))

    xero_bill_line_items = relationship(
        'XeroBillLineItem',
        back_populates='xero_bill',
        cascade='all, delete-orphan'
    )

    def to_dict(self):
        return {
            'id': self.id,
            'state': self.state,
            'xero_reference_number': self.xero_reference_number,
            'xero_link': self.xero_link,
            'xero_id': self.xero_id,
            'transaction_date': self.transaction_date,
            'project_number': self.project_number,
            'po_number': self.po_number,
            'detail_number': self.detail_number,
            'due_date': self.due_date,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }


class XeroBillLineItem(Base):
    __tablename__ = 'xero_bill_line_item'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    project_number = Column(Integer, nullable=True)
    po_number = Column(Integer, nullable=True)
    detail_number = Column(Integer, nullable=False)
    line_number = Column(Integer, nullable=True)
    description = Column(String(255), nullable=True)
    transaction_date = Column(Date, nullable=True)
    due_date = Column(Date, nullable=True)
    quantity = Column(Numeric(10, 0), nullable=True)
    unit_amount = Column(Numeric(10, 0), nullable=True)
    line_amount = Column(Numeric(10, 0), nullable=True)
    tax_code = Column(Integer, nullable=True)
    parent_id = Column(
        BigInteger,
        ForeignKey('xero_bill.id', onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False
    )
    xero_bill_line_id = Column(String(255), nullable=True)
    parent_xero_id = Column(String(255), nullable=True)
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'))
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))

    xero_bill = relationship('XeroBill', back_populates='xero_bill_line_items')

    def to_dict(self):
        return {
            'id': self.id,
            'project_number': self.project_number,
            'po_number': self.po_number,
            'detail_number': self.detail_number,
            'line_number': self.line_number,
            'description': self.description,
            'quantity': self.quantity,
            'unit_amount': self.unit_amount,
            'line_amount': self.line_amount,
            'tax_code': self.tax_code,
            'parent_id': self.parent_id,
            'parent_xero_id': self.parent_xero_id,
            'xero_bill_line_id': self.xero_bill_line_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
#endregion

#region 📦 PO Logs
class PoLog(Base):
    __tablename__ = 'po_log'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    project_number = Column(Integer, nullable=True)
    filename = Column(String(255), nullable=False)
    db_path = Column(String(255), nullable=False)
    status = Column(
        ENUM('PENDING', 'STARTED', 'COMPLETED', 'FAILED', name='po_log_status_enum'),
        nullable=False,
        server_default=text("'PENDING'::po_log_status_enum")
    )
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'))

    def to_dict(self):
        return {
            'id': self.id,
            'project_number': self.project_number,
            'filename': self.filename,
            'db_path': self.db_path,
            'status': self.status,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
#endregion

#region 🗂 Project, PurchaseOrder & DetailItem (Parent/Child)
# Extract the ENUM definition for DetailItem state and disable creation
detail_state_enum = ENUM(
    'PENDING', 'OVERDUE', 'REVIEWED', 'ISSUE', 'RTP',
    'RECONCILED', 'PAID', 'APPROVED', 'SUBMITTED', 'PO MISMATCH', 'VERIFIED',
    name='detail_state_enum',
    schema='public',
    create_type=False  # Prevent SQLAlchemy from attempting to re-create the type
)

class DetailItem(Base):
    __tablename__ = 'detail_item'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    project_number = Column(Integer, nullable=True)
    po_number = Column(Integer, nullable=True)
    detail_number = Column(Integer, nullable=False)
    line_number = Column(Integer, nullable=False)
    account_code = Column(String(45), nullable=False)
    vendor = Column(String(255), nullable=True)
    payment_type = Column(String(45), nullable=True)
    state = Column(
        detail_state_enum,
        nullable=False,
        server_default=text("'PENDING'::detail_state_enum")
    )
    description = Column(String(255), nullable=True)
    transaction_date = Column(DateTime, nullable=True)
    due_date = Column(DateTime, nullable=True)
    rate = Column(Numeric(15, 2), nullable=False)
    quantity = Column(Numeric(15, 2), nullable=False, server_default='1.00')
    ot = Column(Numeric(15, 2), nullable=True, server_default='0.00')
    fringes = Column(Numeric(15, 2), nullable=True, server_default='0.00')
    sub_total = Column(Numeric(15, 2), nullable=True)
    pulse_id = Column(BigInteger, nullable=True)
    xero_id = Column(String(100), nullable=True)
    parent_pulse_id = Column(BigInteger, nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'))

    def to_dict(self):
        return {
            'id': self.id,
            'project_number': self.project_number,
            'po_number': self.po_number,
            'detail_number': self.detail_number,
            'line_number': self.line_number,
            'account_code': self.account_code,
            'vendor': self.vendor,
            'payment_type': self.payment_type,
            'state': self.state,
            'description': self.description,
            'transaction_date': self.transaction_date,
            'due_date': self.due_date,
            'rate': self.rate,
            'quantity': self.quantity,
            'ot': self.ot,
            'fringes': self.fringes,
            'sub_total': self.sub_total,
            'pulse_id': self.pulse_id,
            'xero_id': self.xero_id,
            'parent_pulse_id': self.parent_pulse_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }


class Project(Base):
    __tablename__ = 'project'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=True)
    project_number = Column(Integer, nullable=True)
    name = Column(String(100), nullable=False)
    status = Column(
        ENUM('Active', 'Closed', name='project_status_enum'),
        nullable=False,
        server_default=text("'Active'::project_status_enum")
    )
    tax_ledger = Column(String(45), nullable=True)
    budget_map_id = Column(String(45), nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'))

    purchase_orders = relationship(
        'PurchaseOrder',
        back_populates='project',
        cascade='all, delete-orphan'
    )

    def to_dict(self):
        return {
            'id': self.id,
            'user_id': self.user_id,
            'project_number': self.project_number,
            'name': self.name,
            'status': self.status,
            'tax_ledger': self.tax_ledger,
            'budget_map_id': self.budget_map_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }


class PurchaseOrder(Base):
    __tablename__ = 'purchase_order'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    project_number = Column(Integer, nullable=True)
    po_number = Column(Integer, nullable=False)
    vendor_name = Column(String(100))
    description = Column(String(255), nullable=True)
    po_type = Column(String(45), nullable=True)
    producer = Column(String(100), nullable=True)
    pulse_id = Column(BigInteger, nullable=True)
    folder_link = Column(String(255), nullable=True)
    contact_id = Column(BigInteger, ForeignKey('contact.id', ondelete='SET NULL'), nullable=True)
    project_id = Column(BigInteger, ForeignKey('project.id', onupdate='NO ACTION', ondelete='NO ACTION'), nullable=False)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'))

    __table_args__ = (UniqueConstraint('project_id', 'po_number', name='project_id_po_number'),)

    project = relationship('Project', back_populates='purchase_orders')
    contact = relationship('Contact', back_populates='purchase_orders')

    def to_dict(self):
        return {
            'id': self.id,
            'project_number': self.project_number,
            'po_number': self.po_number,
            'vendor_name': self.vendor_name,
            'description': self.description,
            'po_type': self.po_type,
            'producer': self.producer,
            'pulse_id': self.pulse_id,
            'folder_link': self.folder_link,
            'contact_id': self.contact_id,
            'project_id': self.project_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
#endregion

#region 💸 Invoice, Receipt & SpendMoney
class Invoice(Base):
    __tablename__ = 'invoice'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    project_number = Column(Integer, nullable=False)
    po_number = Column(BigInteger, nullable=False)
    invoice_number = Column(Integer, nullable=False)
    term = Column(Integer, nullable=True)
    total = Column(Numeric(15, 2), nullable=True, server_default='0.00')
    transaction_date = Column(DateTime, nullable=True)
    status = Column(
        ENUM('PENDING', 'VERIFIED', 'REJECTED', name='invoice_status_enum'),
        nullable=True,
        server_default=text("'PENDING'::invoice_status_enum")
    )
    file_link = Column(String(255), nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'))

    def to_dict(self):
        return {
            'id': self.id,
            'project_number': self.project_number,
            'po_number': self.po_number,
            'invoice_number': self.invoice_number,
            'term': self.term,
            'total': self.total,
            'status': self.status,
            'transaction_date': self.transaction_date,
            'file_link': self.file_link,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }


class Receipt(Base):
    __tablename__ = 'receipt'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    project_number = Column(Integer, nullable=False)
    po_number = Column(BigInteger, nullable=True)
    detail_number = Column(Integer, nullable=True)
    line_number = Column(Integer, nullable=True)
    receipt_description = Column(String(255), nullable=True)
    total = Column(Numeric(15, 2), nullable=True, server_default='0.00')
    status = Column(
        ENUM('PENDING', 'VERIFIED', 'REJECTED', name='receipt_status_enum'),
        nullable=True,
        server_default=text("'PENDING'::receipt_status_enum")
    )

    purchase_date = Column(DateTime, nullable=True)
    dropbox_path = Column(String(255), nullable=True)
    file_link = Column(String(255), nullable=False)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'))
    spend_money_id = Column(BigInteger, nullable=True)

    def to_dict(self):
        return {
            'id': self.id,
            'project_number': self.project_number,
            'po_number': self.po_number,
            'detail_number': self.detail_number,
            'line_number': self.line_number,
            'receipt_description': self.receipt_description,
            'status': self.status,
            'total': self.total,
            'purchase_date': self.purchase_date,
            'dropbox_path': self.dropbox_path,
            'file_link': self.file_link,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'spend_money_id': self.spend_money_id
        }


class SpendMoney(Base):
    __tablename__ = 'spend_money'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    project_number = Column(Integer, nullable=False)
    po_number = Column(BigInteger, nullable=False)
    detail_number = Column(Integer, nullable=False)
    line_number = Column(Integer, nullable=False)
    description = Column(String(255), nullable=True)
    contact_id = Column(BigInteger, nullable=True)
    date = Column(DateTime, nullable=True)
    xero_spend_money_id = Column(String(100), nullable=True)
    xero_spend_money_reference_number = Column(String(50), nullable=True, unique=True)
    xero_link = Column(String(255), nullable=True)
    amount = Column(Numeric(15, 2), nullable=False)
    tax_code = Column(Integer, nullable=True)
    state = Column(String(45), nullable=False, server_default='Draft')
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'))

    def to_dict(self):
        return {
            'id': self.id,
            'project_number': self.project_number,
            'po_number': self.po_number,
            'detail_number': self.detail_number,
            'line_number': self.line_number,
            'xero_spend_mondey_id': self.xero_spend_money_id,
            'xero_spend_money_reference_number': self.xero_spend_money_reference_number,
            'xero_link': self.xero_link,
            'date': self.date,
            'state': self.state,
            'contact_id': self.contact_id,
            'description': self.description,
            'tax_code': self.tax_code,
            'amount': self.amount,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
#endregion

#region 🧮 SysTable (the table literally named "sys_table")
class SysTable(Base):
    __tablename__ = 'sys_table'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    type = Column(
        ENUM('SYSTEM', 'PARENT/CHILD', 'PARENT', 'CHILD', 'SINGLE', name='sys_table_type_enum'),
        nullable=False,
        server_default=text("'SINGLE'::sys_table_type_enum")
    )
    integration_name = Column(String(45), nullable=True)
    integration_type = Column(
        ENUM('PARENT', 'CHILD', 'SINGLE', name='integration_type_enum'),
        nullable=False,
        server_default=text("'SINGLE'::integration_type_enum")
    )
    integration_connection = Column(
        ENUM('NONE', '1to1', '1toMany', 'Manyto1', 'ManytoMany', name='integration_conn_enum'),
        nullable=False,
        server_default=text("'NONE'::integration_conn_enum")
    )
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'type': self.type,
            'integration_name': self.integration_name,
            'integration_type': self.integration_type,
            'integration_connection': self.integration_connection,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
#endregion

#region Tax Form
class TaxForm(Base):
    __tablename__ = 'tax_form'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    type = Column(ENUM('W9', 'W8-BEN', 'W8-BEN-E', name='tax_form_type_enum')
 )
    status = Column(
        ENUM('VERIFIED', 'INVALID', 'PENDING', name='tax_form_status_enum'),
        server_default=text("'PENDING'::tax_form_status_enum")
    )
    entity_name = Column(String(100), unique=True, nullable=False)
    filename = Column(String(100))
    db_path = Column(String(255))
    tax_form_link = Column(String(255), nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(
        DateTime,
        server_default=text('CURRENT_TIMESTAMP'),
        onupdate=text('CURRENT_TIMESTAMP')
    )

    def to_dict(self):
        return {
            'id': self.id,
            'status': self.status,
            'type': self.type,
            'entity_name': self.entity_name,
            'filename': self.filename,
            'db_path': self.db_path,
            'tax_form_link': self.tax_form_link,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
#endregion

#region Dropboxfolder
class Dropboxfolder(Base):
    __tablename__ = 'dropbox_folder'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    project_number = Column(Integer, nullable=True)
    po_number = Column(Integer, nullable=True)
    vendor_name = Column(String(100), nullable=True)
    dropbox_path = Column(String(255), nullable=True)
    share_link = Column(String(255), nullable=True)

    def to_dict(self):
        return {
            'id': self.id,
            'project_number': self.project_number,
            'po_number': self.po_number,
            'vendor_name': self.vendor_name,
            'dropbox_path': self.dropbox_path,
            'share_link': self.share_link,
        }
#endregion