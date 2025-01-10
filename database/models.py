# database.models.py

import logging

from sqlalchemy import (
    Column,
    String,
    DateTime,
    ForeignKey,
    Enum,
    UniqueConstraint,
    Index,
    Computed,
    text
)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import (
    INTEGER as MYSQL_INTEGER,
    DECIMAL as MYSQL_DECIMAL,
    BIGINT as MYSQL_BIGINT,
)

Base = declarative_base()

# Configure logging levels for SQLAlchemy
logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.ERROR)
logging.getLogger("sqlalchemy.pool").setLevel(logging.ERROR)


# -------------------------------------------------------------------
# PROJECT
# -------------------------------------------------------------------
class Project(Base):
    """
    Corresponds to table: project
    """
    __tablename__ = 'project'

    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    project_number = Column(MYSQL_INTEGER, nullable=True)
    name = Column(String(100), nullable=False)
    status = Column(Enum('Active', 'Closed'), nullable=False, server_default='Active')
    total_spent = Column(MYSQL_DECIMAL(10, 2), nullable=True, server_default='0.00')
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(
        DateTime,
        server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')
    )

    # Relationships
    purchase_orders = relationship(
        'PurchaseOrder',
        back_populates='project',
        cascade="all, delete-orphan"
    )


# -------------------------------------------------------------------
# CONTACT
# -------------------------------------------------------------------
class Contact(Base):
    """
    Corresponds to table: contact
    """
    __tablename__ = 'contact'

    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    pulse_id = Column(MYSQL_BIGINT, unique=True)
    name = Column(String(255), nullable=False, index=True)
    vendor_type = Column(String(45), nullable=True)
    payment_details = Column(String(255), nullable=False, server_default='PENDING')
    email = Column(String(100), nullable=True)
    phone = Column(String(45), nullable=True)
    tax_number = Column(String(45), nullable=True)
    address_line_1 = Column(String(255), nullable=True)
    address_line_2 = Column(String(255), nullable=True)
    city = Column(String(100), nullable=True)
    zip = Column(String(20), nullable=True)
    tax_form_link = Column(String(255), nullable=True)
    vendor_status = Column(Enum('PENDING', 'TO VERIFY', 'APPROVED', 'ISSUE'), nullable=False, server_default='PENDING')
    region = Column(String(45), nullable=True)
    country = Column(String(100), nullable=True)
    tax_type = Column(String(45), nullable=True, server_default='SSN')
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    # One-to-many: a contact can have multiple purchase orders
    purchase_orders = relationship(
        'PurchaseOrder',
        back_populates='contact',
        cascade="all, delete-orphan"
    )


# -------------------------------------------------------------------
# PURCHASE_ORDER
# -------------------------------------------------------------------
class PurchaseOrder(Base):
    """
    Corresponds to table: purchase_order
    """
    __tablename__ = 'purchase_order'

    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    project_id = Column(
        MYSQL_INTEGER(unsigned=True),
        ForeignKey('project.id', onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False
    )
    po_number = Column(MYSQL_INTEGER(unsigned=True), nullable=False)
    description = Column(String(255), nullable=True)
    po_type = Column(String(45), nullable=True)
    state = Column(
        Enum('APPROVED', 'TO VERIFY', 'ISSUE', 'PENDING', 'CC / PC', 'PAID'),
        nullable=False,
        server_default='PENDING'
    )
    amount_total = Column(MYSQL_DECIMAL(15, 2), nullable=False, server_default='0.00')
    producer = Column(String(100), nullable=True)
    pulse_id = Column(MYSQL_BIGINT, nullable=True)
    folder_link = Column(String(255), nullable=True)
    contact_id = Column(
        MYSQL_INTEGER(unsigned=True),
        ForeignKey('contact.id', onupdate='CASCADE', ondelete='SET NULL'),
        nullable=True
    )
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    __table_args__ = (
        UniqueConstraint('project_id', 'po_number', name='project_id_po_number'),
    )

    # Relationships
    project = relationship('Project', back_populates='purchase_orders')
    contact = relationship('Contact', back_populates='purchase_orders')
    detail_items = relationship(
        'DetailItem',
        back_populates='purchase_order',
        cascade="all, delete-orphan"
    )

    def to_dict(self):
        return {
            "project_id": self.project_id,
            "po_number": self.po_number,
            "amount_total": self.amount_total,
            "folder": self.folder_link,
            "description": self.description,
        }


# -------------------------------------------------------------------
# DETAIL_ITEM
# -------------------------------------------------------------------
class DetailItem(Base):
    """
    Corresponds to table: detail_item
    """
    __tablename__ = 'detail_item'

    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    po_id = Column(
        MYSQL_INTEGER(unsigned=True),
        ForeignKey('purchase_order.id', onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False
    )
    state = Column(
        Enum('PENDING','OVERDUE','ISSUE','RTP','RECONCILED','PAID','APPROVED', 'REVIEWED', 'SUBMITTED', 'PO MISMATCH'),
        nullable=False,
        server_default='PENDING'
    )
    detail_number = Column(MYSQL_INTEGER(unsigned=True), nullable=False)
    line_id = Column(MYSQL_INTEGER(unsigned=True), nullable=False)
    aicp_code_id = Column(
        MYSQL_INTEGER(unsigned=True),
        ForeignKey('aicp_code.id', onupdate='NO ACTION', ondelete='NO ACTION'),
        nullable=False
    )
    vendor = Column(String(255), nullable=True)
    description = Column(String(255), nullable=True)
    transaction_date = Column(DateTime, nullable=True)
    due_date = Column(DateTime, nullable=True)
    rate = Column(MYSQL_DECIMAL(15, 2), nullable=False)
    quantity = Column(MYSQL_DECIMAL(15, 2), nullable=False, server_default='1.00')
    ot = Column(MYSQL_DECIMAL(15, 2), nullable=True, server_default='0.00')
    fringes = Column(MYSQL_DECIMAL(15, 2), nullable=True, server_default='0.00')
    pulse_id = Column(MYSQL_BIGINT, nullable=True)
    parent_pulse_id = Column(MYSQL_BIGINT, nullable=True)
    receipt_id = Column(MYSQL_INTEGER(unsigned=True))
    payment_type = Column(String(45), nullable=True)
    invoice_id = Column(MYSQL_INTEGER(unsigned=True))
    sub_total = Column(
        MYSQL_DECIMAL(15, 2),
        Computed("ROUND(((rate * quantity) + IFNULL(ot,0) + IFNULL(fringes,0)),2)", persisted=True),
        nullable=False
    )
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    # Relationship
    purchase_order = relationship('PurchaseOrder', back_populates='detail_items')


# -------------------------------------------------------------------
# INVOICE
# -------------------------------------------------------------------
class Invoice(Base):
    """
    Corresponds to table: invoice
    """
    __tablename__ = 'invoice'

    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    transaction_date = Column(DateTime, nullable=True)
    term = Column(MYSQL_INTEGER, nullable=True)
    total = Column(MYSQL_DECIMAL(15, 2), nullable=True, server_default='0.00')
    file_link = Column(String(255), nullable=True)
    po_number = Column(
        MYSQL_INTEGER(unsigned=True),
        nullable=False
    )
    project_number = Column(
        MYSQL_INTEGER(unsigned=True),
        nullable=False
    )
    invoice_number = Column(MYSQL_INTEGER(unsigned=True), nullable=False)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))




# -------------------------------------------------------------------
# XERO_BILL
# -------------------------------------------------------------------
class XeroBill(Base):
    """
    Corresponds to table: xero_bill
    """
    __tablename__ = 'xero_bill'

    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    state = Column(String(45), nullable=False, server_default='Draft')
    xero_reference_number = Column(String(45), nullable=True, unique=True)
    xero_link = Column(String(255))
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    # Relationship
    bill_line_items = relationship(
        'BillLineItem',
        back_populates='xero_bill',
        cascade="all, delete-orphan"
    )


# -------------------------------------------------------------------
# BANK_TRANSACTION
# -------------------------------------------------------------------
class BankTransaction(Base):
    """
    Corresponds to table: bank_transaction
    """
    __tablename__ = 'bank_transaction'

    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    mercury_transaction_id = Column(String(100), nullable=False, unique=True)
    state = Column(String(45), nullable=False, server_default='Pending')
    xero_bill_id = Column(
        MYSQL_INTEGER(unsigned=True),
        ForeignKey('xero_bill.id', onupdate='CASCADE', ondelete='NO ACTION'),
        nullable=False
    )
    xero_spend_money_id = Column(
        MYSQL_INTEGER(unsigned=True),
        ForeignKey('spend_money.id', onupdate='CASCADE', ondelete='NO ACTION'),
        nullable=False
    )
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    # Relationships
    xero_bill = relationship('XeroBill')
    spend_money = relationship('SpendMoney')


# -------------------------------------------------------------------
# AICP_CODE
# -------------------------------------------------------------------
class AicpCode(Base):
    """
    Corresponds to table: aicp_code
    """
    __tablename__ = 'aicp_code'

    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    aicp_code = Column(String(45), nullable=False, unique=True)
    tax_id = Column(
        MYSQL_INTEGER(unsigned=True),
        ForeignKey('tax_account.id', onupdate='NO ACTION', ondelete='NO ACTION'),
        nullable=False
    )
    aicp_description = Column(String(45), nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    # Relationship
    tax_account = relationship('TaxAccount')
    # If needed: detail_items = relationship('DetailItem', back_populates='aicp_code_rel')


# -------------------------------------------------------------------
# TAX_ACCOUNT
# -------------------------------------------------------------------
class TaxAccount(Base):
    """
    Corresponds to table: tax_account
    """
    __tablename__ = 'tax_account'

    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    tax_code = Column(String(45), nullable=False, unique=True)
    description = Column(String(255), nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
    # If needed: aicp_codes = relationship('AicpCode', back_populates='tax_account')


# -------------------------------------------------------------------
# BILL_LINE_ITEM
# -------------------------------------------------------------------
class BillLineItem(Base):
    """
    Corresponds to table: bill_line_item
    """
    __tablename__ = 'bill_line_item'

    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    xero_bill_id = Column(
        MYSQL_INTEGER(unsigned=True),
        ForeignKey('xero_bill.id', onupdate='NO ACTION', ondelete='NO ACTION'),
        nullable=False
    )
    detail_item_id = Column(
        MYSQL_INTEGER(unsigned=True),
        nullable=True
    )
    description = Column(String(255), nullable=True)
    quantity = Column(MYSQL_DECIMAL, nullable=True)
    unit_amount = Column(MYSQL_DECIMAL, nullable=True)
    line_amount = Column(MYSQL_DECIMAL, nullable=True)
    account_code = Column(MYSQL_INTEGER, nullable=True)
    xero_id = Column(String(255), nullable=False)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    # Relationship
    xero_bill = relationship('XeroBill', back_populates='bill_line_items')


# -------------------------------------------------------------------
# RECEIPT
# -------------------------------------------------------------------
class Receipt(Base):
    """
    Corresponds to table: receipt
    """
    __tablename__ = 'receipt'

    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    total = Column(MYSQL_DECIMAL(15, 2), nullable=True, server_default='0.00')
    receipt_description = Column(String(255), nullable=True)
    purchase_date = Column(DateTime, nullable=True)
    file_link = Column(String(255), nullable=False)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    spend_money_id = Column(
        MYSQL_INTEGER(unsigned=True),
        nullable=True
    )
    project_number = Column(
        MYSQL_INTEGER(unsigned=True),
        nullable=False
    )
    po_number = Column(
        MYSQL_INTEGER(unsigned=True),
        nullable=False
    )
    detail_number = Column(
        MYSQL_INTEGER(unsigned=True),
        nullable=False
    )
    line_id = Column(
        MYSQL_INTEGER(unsigned=True),
        nullable=False
    )



# -------------------------------------------------------------------
# SPEND_MONEY
# -------------------------------------------------------------------
class SpendMoney(Base):
    """
    Corresponds to table: spend_money
    """
    __tablename__ = 'spend_money'

    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    xero_spend_money_reference_number = Column(String(100), nullable=True, unique=True)
    xero_link = Column(String(255), nullable=True)
    state = Column(String(45), nullable=False, server_default='Draft')
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    bank_transactions = relationship(
        'BankTransaction',
        back_populates='spend_money',
        cascade="all, delete-orphan",
        uselist=True
    )
    # Optionally: receipts = relationship('Receipt', back_populates='spend_money')