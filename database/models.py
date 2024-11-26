# models.py

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Date,
    ForeignKey,
    Text,
    Boolean,
    DECIMAL,
    UniqueConstraint,
    Enum,
    ForeignKeyConstraint,
    BigInteger,
    Index,
)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

# If you have a separate base.py, import Base from there
# from database.base import Base
Base = declarative_base()

import logging

logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.ERROR)
logging.getLogger("sqlalchemy.pool").setLevel(logging.ERROR)


# TaxAccount Model
class TaxAccount(Base):
    __tablename__ = 'tax_accounts'

    tax_code_id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(45), nullable=False, unique=True)
    description = Column(String(255), nullable=True)

    # Relationships
    aicp_codes = relationship('AicpCode', back_populates='tax_account')
    bill_line_items = relationship('BillLineItem', back_populates='tax_account')
    spend_money_transactions = relationship('SpendMoney', back_populates='tax_account')


# AicpCode Model
class AicpCode(Base):
    __tablename__ = 'aicp_codes'

    account_code_id = Column(Integer, primary_key=True, autoincrement=True)
    line_number = Column(String(45), nullable=False, unique=True)
    tax_code_id = Column(Integer, ForeignKey('tax_accounts.tax_code_id', onupdate='CASCADE', ondelete='RESTRICT'), nullable=False)
    description = Column(String(45), nullable=True)

    # Relationships
    tax_account = relationship('TaxAccount', back_populates='aicp_codes')
    detail_items = relationship('DetailItem', back_populates='account_code')


# Contact Model
class Contact(Base):
    __tablename__ = 'contacts'

    contact_surrogate_id = Column(BigInteger, primary_key=True)
    pulse_id = Column(BigInteger, nullable=False)
    name = Column(String(100), nullable=False, index=True)
    vendor_type = Column(String(45), nullable=True, index=True)
    payment_details = Column(String(255), nullable=False, default="PENDING")
    email = Column(String(100), nullable=True, unique=True)
    phone = Column(String(45), nullable=True)
    address_line_1 = Column(String(255), nullable=True)
    city = Column(String(100), nullable=True)
    zip = Column(String(20), nullable=True)
    tax_ID = Column(String(45), nullable=True, unique=True)
    tax_form_link = Column(String(255), nullable=True)
    tax_form_status = Column(String(45), nullable=False, default="PENDING")

    # Relationships
    purchase_orders = relationship('PurchaseOrder', back_populates='contact')


# Project Model
class Project(Base):
    __tablename__ = 'projects'

    project_id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String(100), nullable=False)
    status = Column(Enum('Active', 'Closed'), nullable=False)

    # Relationships
    purchase_orders = relationship('PurchaseOrder', back_populates='project')


# PurchaseOrder Model
class PurchaseOrder(Base):
    __tablename__ = 'purchase_orders'

    po_surrogate_id = Column(BigInteger, primary_key=True)
    state = Column(String(45), nullable=False, default='PENDING')
    amount_total = Column(DECIMAL(15, 2), nullable=False, default=0.00)
    description = Column(String(255), nullable=True)
    folder_link = Column(String(255), nullable=True)
    producer = Column(String(100), nullable=True)
    po_type = Column(String(45), nullable=False)
    pulse_id = Column(BigInteger, nullable=False, unique=True)
    issue_type = Column(String(45), nullable=True)
    tax_form_link = Column(String(255), nullable=True)
    contact_id = Column(BigInteger, ForeignKey('contacts.contact_surrogate_id', onupdate='CASCADE', ondelete='SET NULL'), nullable=True)
    project_id = Column(Integer, ForeignKey('projects.project_id', onupdate='CASCADE', ondelete='RESTRICT'), nullable=False)
    po_number = Column(BigInteger, nullable=False)

    __table_args__ = (
        UniqueConstraint('project_id', 'po_number', name='project_id_po_number'),
    )

    # Indexes
    Index('purchase_orders_pulse_id_uindex', 'pulse_id', unique=True)
    Index('fk_purchase_orders_contacts1_idx', 'contact_id')

    # Relationships
    project = relationship('Project', back_populates='purchase_orders')
    contact = relationship('Contact', back_populates='purchase_orders')
    detail_items = relationship('DetailItem', back_populates='purchase_order')
    invoices = relationship('Invoice', back_populates='purchase_order')


# Invoice Model
class Invoice(Base):
    __tablename__ = 'invoices'

    invoice_surrogate_id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_date = Column(DateTime, nullable=True)
    term = Column(Integer, nullable=True)
    total = Column(DECIMAL(15, 2), nullable=True)
    invoice_number = Column(Integer, nullable=False)
    file_link = Column(String(255), nullable=True)
    po_surrogate_id = Column(BigInteger, ForeignKey('purchase_orders.po_surrogate_id', onupdate='CASCADE', ondelete='RESTRICT'), nullable=False)

    __table_args__ = (
        UniqueConstraint('po_surrogate_id', 'invoice_number', name='po_surrogate_id_invoice_number_UNIQUE'),
    )

    # Relationships
    purchase_order = relationship('PurchaseOrder', back_populates='invoices')
    xero_bills = relationship('XeroBill', back_populates='invoice')


# XeroBill Model
class XeroBill(Base):
    __tablename__ = 'xero_bills'

    bill_surrogate_id = Column(Integer, primary_key=True, autoincrement=True)
    state = Column(String(45), nullable=False, default='Draft')
    invoice_id = Column(Integer, ForeignKey('invoices.invoice_surrogate_id', onupdate='CASCADE', ondelete='CASCADE'), nullable=False)
    xero_id = Column(String(45), nullable=True, unique=True)

    # Indexes
    Index('fk_invoice_id_idx', 'invoice_id')

    # Relationships
    invoice = relationship('Invoice', back_populates='xero_bills')
    bank_transactions = relationship('BankTransaction', back_populates='xero_bill')
    bill_line_items = relationship('BillLineItem', back_populates='xero_bill')


# BankTransaction Model
class BankTransaction(Base):
    __tablename__ = 'bank_transactions'

    bank_transaction_id = Column(Integer, primary_key=True, autoincrement=True)
    mercury_transaction_id = Column(String(100), nullable=False, unique=True)
    state = Column(String(45), nullable=False, default='Pending')
    bill_id = Column(Integer, ForeignKey('xero_bills.bill_surrogate_id', ondelete='CASCADE'), nullable=False)

    # Indexes
    Index('fk_bill_id_idx', 'bill_id')

    # Relationships
    xero_bill = relationship('XeroBill', back_populates='bank_transactions')


# DetailItem Model
class DetailItem(Base):
    __tablename__ = 'detail_items'

    detail_item_surrogate_id = Column(BigInteger, primary_key=True)
    transaction_date = Column(DateTime, nullable=True)
    due_date = Column(DateTime, nullable=True)
    rate = Column(DECIMAL(15, 2), nullable=False)
    quantity = Column(DECIMAL(15, 2), nullable=False, default=1.00)
    description = Column(String(255), nullable=True)
    file_link = Column(String(255), nullable=True, comment='Link to invoice or receipt in Dropbox')
    is_receipt = Column(Boolean, nullable=False, default=False, comment='1 for receipts, 0 for invoices')
    state = Column(String(45), nullable=False, default='Pending')
    pulse_id = Column(BigInteger, nullable=True, unique=True, comment='Integration with Monday.com')
    issue_type = Column(String(45), nullable=True, comment="'PO_LOG_MISMATCH', 'OCR_ERROR', etc.")
    account_number_id = Column(Integer, ForeignKey('aicp_codes.account_code_id'), nullable=False)
    project_id = Column(Integer, nullable=False)
    po_number = Column(BigInteger, nullable=False)
    detail_item_id = Column(Integer, nullable=False)

    __table_args__ = (
        UniqueConstraint('detail_item_id', 'po_number', 'project_id', name='project_id_po_number_detail_id'),
        ForeignKeyConstraint(
            ['project_id', 'po_number'],
            ['purchase_orders.project_id', 'purchase_orders.po_number'],
            onupdate='CASCADE',
            ondelete='CASCADE',
            name='project_id_po_number',
        ),
    )

    # Indexes
    Index('detail_items_pulse_id_uindex', 'pulse_id', unique=True)
    Index('project_id_po_number_idx', 'project_id', 'po_number')
    Index('account_number_idx', 'account_number_id')

    # Relationships
    purchase_order = relationship('PurchaseOrder', back_populates='detail_items', primaryjoin='and_(DetailItem.project_id == PurchaseOrder.project_id, DetailItem.po_number == PurchaseOrder.po_number)')
    account_code = relationship('AicpCode', back_populates='detail_items')
    bill_line_items = relationship('BillLineItem', back_populates='detail_item')
    receipt = relationship('Receipt', back_populates='detail_item', uselist=False)

    @hybrid_property
    def sub_total(self):
        return round(self.rate * self.quantity, 2)


# BillLineItem Model
class BillLineItem(Base):
    __tablename__ = 'bill_line_items'

    bill_line_surrogate_id = Column(Integer, primary_key=True, autoincrement=True)
    bill_id = Column(Integer, ForeignKey('xero_bills.bill_surrogate_id', onupdate='CASCADE', ondelete='CASCADE'), nullable=False)
    tax_account_id = Column(Integer, ForeignKey('tax_accounts.tax_code_id'), nullable=False)
    detail_item_id = Column(BigInteger, ForeignKey('detail_items.detail_item_surrogate_id', onupdate='CASCADE', ondelete='CASCADE'), nullable=False, unique=True)

    # Indexes
    Index('fk_bill_line_items_bills_idx', 'bill_id')
    Index('fk_tax_code_id_idx', 'tax_account_id')

    # Relationships
    xero_bill = relationship('XeroBill', back_populates='bill_line_items')
    detail_item = relationship('DetailItem', back_populates='bill_line_items')
    tax_account = relationship('TaxAccount', back_populates='bill_line_items')


# Receipt Model
class Receipt(Base):
    __tablename__ = 'receipt'

    receipt_surrogate_id = Column(BigInteger, primary_key=True)
    total = Column(DECIMAL(15, 2), nullable=True)
    receipt_description = Column(String(45), nullable=True)
    purchase_date = Column(DateTime, nullable=True)
    project_id = Column(Integer, nullable=False)
    po_number = Column(Integer, nullable=False)
    receipt_number = Column(Integer, nullable=False)
    file_link = Column(String(255), nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ['receipt_surrogate_id'],
            ['detail_items.detail_item_surrogate_id'],
            onupdate='CASCADE',
            ondelete='RESTRICT',
            name='fk_detail_surrogate_id',
        ),
    )

    # Relationships
    detail_item = relationship('DetailItem', back_populates='receipt', uselist=False)
    spend_money = relationship('SpendMoney', back_populates='receipt', uselist=False)


# SpendMoney Model
class SpendMoney(Base):
    __tablename__ = 'spend_money'

    spend_money_surrogate_id = Column(Integer, primary_key=True, autoincrement=True)
    xero_spend_money_id = Column(String(100), nullable=True, unique=True)
    tax_account_id = Column(Integer, ForeignKey('tax_accounts.tax_code_id', onupdate='CASCADE', ondelete='RESTRICT'), nullable=False)
    file_link = Column(String(255), nullable=True)
    state = Column(String(45), nullable=False, default='Draft')
    receipt_id = Column(BigInteger, ForeignKey('receipt.receipt_surrogate_id', onupdate='CASCADE', ondelete='CASCADE'), nullable=False, unique=True)

    # Indexes
    Index('fk_tax_account_id_idx', 'tax_account_id')
    Index('fk_receipt_id_idx', 'receipt_id')

    # Relationships
    tax_account = relationship('TaxAccount', back_populates='spend_money_transactions')
    receipt = relationship('Receipt', back_populates='spend_money', uselist=False)