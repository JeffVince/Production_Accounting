#region 🚀 Imports
import logging
from sqlalchemy import (
    Column, String, DateTime, ForeignKey, Enum, UniqueConstraint, Index,
    Computed, text, Date
)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import (
    INTEGER as MYSQL_INTEGER, DECIMAL as MYSQL_DECIMAL, BIGINT as MYSQL_BIGINT
)

logging.getLogger('sqlalchemy.engine.Engine').setLevel(logging.ERROR)
logging.getLogger('sqlalchemy.pool').setLevel(logging.ERROR)

Base = declarative_base()
#endregion

#region 📄 Contact & User
class Contact(Base):
    __tablename__ = 'contact'
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, index=True)
    vendor_status = Column(
        Enum('PENDING', 'TO VERIFY', 'APPROVED', 'ISSUE'),
        nullable=False,
        server_default='PENDING'
    )
    payment_details = Column(String(255), nullable=False, server_default='PENDING')
    vendor_type = Column(String(45), nullable=True)
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
    tax_form_link = Column(String(255), nullable=True)
    pulse_id = Column(MYSQL_BIGINT, unique=True)
    xero_id = Column(String(255))
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

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
            'tax_form_link': self.tax_form_link,
            'pulse_id': self.pulse_id,
            'xero_id': self.xero_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }


class User(Base):
    __tablename__ = 'users'
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    username = Column(String(100), nullable=False)
    contact_id = Column(MYSQL_INTEGER(unsigned=True),
                        ForeignKey('contact.id', onupdate='CASCADE', ondelete='SET NULL'),
                        nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

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
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    map_name = Column(String(100), nullable=False)
    user_id = Column(MYSQL_INTEGER(unsigned=True),
                     ForeignKey('users.id', onupdate='CASCADE', ondelete='CASCADE'),
                     nullable=False)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

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
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    code = Column(String(45), nullable=False, unique=False)
    budget_map_id = Column(MYSQL_INTEGER(unsigned=True),
                           ForeignKey('budget_map.id', onupdate='CASCADE', ondelete='SET NULL'),
                           nullable=True)
    tax_id = Column(MYSQL_INTEGER(unsigned=True),
                    ForeignKey('tax_account.id', onupdate='CASCADE', ondelete='SET NULL'),
                    nullable=True)
    account_description = Column(String(45), nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    # Relationship to tax_account if needed
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
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    user_id = Column(MYSQL_INTEGER(unsigned=True),
                     ForeignKey('users.id', onupdate='CASCADE', ondelete='CASCADE'),
                     nullable=False)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

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
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    tax_code = Column(String(45), nullable=False, unique=False)  # DB has no unique on tax_code, just on id
    description = Column(String(255), nullable=True)
    tax_ledger_id = Column(MYSQL_INTEGER(unsigned=True),
                           ForeignKey('tax_ledger.id', onupdate='CASCADE', ondelete='SET NULL'),
                           nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

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
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    table_id = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    operation = Column(String(10), nullable=False)
    record_id = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
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
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    mercury_transaction_id = Column(String(100), nullable=False, unique=True)
    state = Column(String(45), nullable=False, server_default='Pending')
    xero_bill_id = Column(MYSQL_INTEGER(unsigned=True),
                          ForeignKey('xero_bill.id', onupdate='NO ACTION', ondelete='NO ACTION'),
                          nullable=False)
    xero_spend_money_id = Column(MYSQL_INTEGER(unsigned=True),
                                 ForeignKey('spend_money.id', onupdate='NO ACTION', ondelete='NO ACTION'),
                                 nullable=False)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    # Relationships
    xero_bill = relationship('XeroBill')
    spend_money = relationship('SpendMoney')

    def to_dict(self):
        return {
            'id': self.id,
            'mercury_transaction_id': self.mercury_transaction_id,
            'state': self.state,
            'xero_bill_id': self.xero_bill_id,
            'xero_spend_money_id': self.xero_spend_money_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
#endregion

#region 📑 XeroBill & BillLineItem (Parent/Child)
class XeroBill(Base):
    __tablename__ = 'xero_bill'
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    state = Column(String(45), nullable=False, server_default='Draft')
    project_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    po_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    detail_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    transaction_date = Column(Date, nullable=True)  # Matching DATE in MySQL
    due_date = Column(Date, nullable=True)          # Matching DATE in MySQL
    xero_id = Column(String(255), nullable=True)
    xero_link = Column(String(255), nullable=True)
    xero_reference_number = Column(String(45), nullable=True, unique=False)
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))

    bill_line_items = relationship(
        'BillLineItem',
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


class BillLineItem(Base):
    __tablename__ = 'bill_line_item'
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    project_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    po_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    detail_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    line_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    description = Column(String(255), nullable=True)
    quantity = Column(MYSQL_DECIMAL(10, 0), nullable=True)
    unit_amount = Column(MYSQL_DECIMAL(10, 0), nullable=True)
    line_amount = Column(MYSQL_DECIMAL(10, 0), nullable=True)
    account_code = Column(MYSQL_INTEGER, nullable=True)
    parent_id = Column(
        MYSQL_INTEGER(unsigned=True),
        ForeignKey('xero_bill.id', onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False
    )
    xero_bill_line_id = Column(String(255), nullable=True)
    parent_xero_id = Column(String(255), nullable=True)
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))

    xero_bill = relationship('XeroBill', back_populates='bill_line_items')

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
            'account_code': self.account_code,
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

    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    project_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    filename = Column(String(255), nullable=False)
    db_path = Column(String(255), nullable=False)
    status = Column(
        Enum('PENDING', 'STARTED', 'COMPLETED', 'FAILED'),
        nullable=False,
        server_default='PENDING'
    )
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(
        DateTime,
        server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')
    )

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
class DetailItem(Base):
    __tablename__ = 'detail_item'
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    project_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    po_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    detail_number = Column(MYSQL_INTEGER(unsigned=True), nullable=False)
    line_number = Column(MYSQL_INTEGER(unsigned=True), nullable=False)
    account_code = Column(String(45), nullable=False)
    vendor = Column(String(255), nullable=True)
    payment_type = Column(String(45), nullable=True)
    state = Column(Enum(
        'PENDING', 'OVERDUE', 'REVIEWED', 'ISSUE', 'RTP',
        'RECONCILED', 'PAID', 'APPROVED', 'SUBMITTED', 'PO MISMATCH'
    ), nullable=False, server_default='PENDING')
    description = Column(String(255), nullable=True)
    transaction_date = Column(DateTime, nullable=True)
    due_date = Column(DateTime, nullable=True)
    rate = Column(MYSQL_DECIMAL(15, 2), nullable=False)
    quantity = Column(MYSQL_DECIMAL(15, 2), nullable=False, server_default='1.00')
    ot = Column(MYSQL_DECIMAL(15, 2), nullable=True, server_default='0.00')
    fringes = Column(MYSQL_DECIMAL(15, 2), nullable=True, server_default='0.00')
    sub_total = Column(
        MYSQL_DECIMAL(15, 2),
        Computed(
            'ROUND(((rate * quantity) + IFNULL(ot,0) + IFNULL(fringes,0)),2)',
            persisted=True
        ),
        nullable=False
    )
    receipt_id = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    invoice_id = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    pulse_id = Column(MYSQL_BIGINT, nullable=True)
    parent_pulse_id = Column(MYSQL_BIGINT, nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

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
            'receipt_id': self.receipt_id,
            'invoice_id': self.invoice_id,
            'pulse_id': self.pulse_id,
            'parent_pulse_id': self.parent_pulse_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }


class Project(Base):
    __tablename__ = 'project'
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    user_id = Column(MYSQL_INTEGER, nullable=True)  # The new SQL doesn't define a FK constraint
    project_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    name = Column(String(100), nullable=False)
    status = Column(Enum('Active', 'Closed'), nullable=False, server_default='Active')
    tax_ledger = Column(String(45), nullable=True)
    budget_map_id = Column(String(45), nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

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
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    project_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    po_number = Column(MYSQL_INTEGER(unsigned=True), nullable=False)
    description = Column(String(255), nullable=True)
    po_type = Column(String(45), nullable=True)
    producer = Column(String(100), nullable=True)
    pulse_id = Column(MYSQL_BIGINT, nullable=True)
    folder_link = Column(String(255), nullable=True)
    contact_id = Column(MYSQL_INTEGER(unsigned=True),
                        ForeignKey('contact.id', ondelete='SET NULL'),
                        nullable=True)
    project_id = Column(MYSQL_INTEGER(unsigned=True),
                        ForeignKey('project.id', onupdate='NO ACTION', ondelete='NO ACTION'),
                        nullable=False)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    # Unique index on (project_id, po_number).
    __table_args__ = (
        UniqueConstraint('project_id', 'po_number', name='project_id_po_number'),
    )

    project = relationship('Project', back_populates='purchase_orders')
    contact = relationship('Contact', back_populates='purchase_orders')

    def to_dict(self):
        return {
            'id': self.id,
            'project_number': self.project_number,
            'po_number': self.po_number,
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
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    project_number = Column(MYSQL_INTEGER(unsigned=True), nullable=False)
    po_number = Column(MYSQL_INTEGER(unsigned=True), nullable=False)
    invoice_number = Column(MYSQL_INTEGER(unsigned=True), nullable=False)
    term = Column(MYSQL_INTEGER, nullable=True)
    total = Column(MYSQL_DECIMAL(15, 2), nullable=True, server_default='0.00')
    transaction_date = Column(DateTime, nullable=True)
    file_link = Column(String(255), nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    def to_dict(self):
        return {
            'id': self.id,
            'project_number': self.project_number,
            'po_number': self.po_number,
            'invoice_number': self.invoice_number,
            'term': self.term,
            'total': self.total,
            'transaction_date': self.transaction_date,
            'file_link': self.file_link,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }


class Receipt(Base):
    __tablename__ = 'receipt'
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    project_number = Column(MYSQL_INTEGER(unsigned=True), nullable=False)
    po_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    detail_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    line_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    receipt_description = Column(String(255), nullable=True)
    total = Column(MYSQL_DECIMAL(15, 2), nullable=True, server_default='0.00')
    purchase_date = Column(DateTime, nullable=True)
    dropbox_path = Column(String(255), nullable=True)
    file_link = Column(String(255), nullable=False)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
    spend_money_id = Column(MYSQL_INTEGER(unsigned=True), nullable=True)

    def to_dict(self):
        return {
            'id': self.id,
            'project_number': self.project_number,
            'po_number': self.po_number,
            'detail_number': self.detail_number,
            'line_number': self.line_number,
            'receipt_description': self.receipt_description,
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
    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    project_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    po_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    detail_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    line_number = Column(MYSQL_INTEGER(unsigned=True), nullable=True)
    xero_spend_money_reference_number = Column(String(100), nullable=True, unique=True)
    xero_link = Column(String(255), nullable=True)
    state = Column(String(45), nullable=False, server_default='Draft')
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    def to_dict(self):
        return {
            'id': self.id,
            'project_number': self.project_number,
            'po_number': self.po_number,
            'detail_number': self.detail_number,
            'line_number': self.line_number,
            'xero_spend_money_reference_number': self.xero_spend_money_reference_number,
            'xero_link': self.xero_link,
            'state': self.state,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
#endregion

#region 🧮 Table (the table literally named "table")
class Table(Base):  # Represents the table literally named "table"
    __tablename__ = 'table'
    # Note: your SQL has it as "id int not null auto_increment" (not unsigned)
    id = Column(MYSQL_INTEGER(unsigned=False), primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    type = Column(Enum('SYSTEM', 'PARENT', 'CHILD', 'SINGLE', 'PARENT/CHILD'), nullable=False, server_default='SINGLE')
    integration_name = Column(String(45), nullable=True)
    integration_type = Column(Enum('PARENT', 'CHILD', 'SINGLE'), nullable=False, server_default='SINGLE')
    integration_connection = Column(
        Enum('NONE', '1to1', '1toMany', 'Manyto1', 'ManytoMany'),
        nullable=False,
        server_default='NONE'
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
#endregion_q