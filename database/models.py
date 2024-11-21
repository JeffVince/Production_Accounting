# database/models.py

from sqlalchemy import (
    Column, Integer, String, Float, DateTime, ForeignKey, Enum, Text
)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import enum
from datetime import datetime

Base = declarative_base()


# Enums for state fields
class POState(enum.Enum):
    PENDING = 'PENDING'
    RTP = 'RTP'
    TO_VERIFY = 'TO_VERIFY'
    ISSUE = 'ISSUE'
    APPROVED = 'APPROVED'
    PAID = 'PAID'
    RECONCILED = 'RECONCILED'


class POSubitemState(enum.Enum):
    PENDING = 'PENDING'
    PAID = 'PAID'
    ISSUE = 'ISSUE'
    RECONCILED = 'RECONCILED'


class BillState(enum.Enum):
    DRAFT = 'Draft'
    SUBMITTED = 'Submitted'
    APPROVED = 'Approved'
    PAID = 'Paid'
    RECONCILED = 'Reconciled'


class SpendMoneyState(enum.Enum):
    DRAFT = 'Draft'
    APPROVED = 'Approved'
    RECONCILED = 'Reconciled'


class TransactionState(enum.Enum):
    PENDING = 'PENDING'
    PAID = 'PAID'
    CONFIRMED = 'CONFIRMED'


# Project Model
class Project(Base):
    __tablename__ = 'projects'

    id = Column(Integer, primary_key=True)
    project_id = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)

    pos = relationship('PO', back_populates='project')


# PO (Purchase Order) Model
class PO(Base):
    __tablename__ = 'pos'

    id = Column(Integer, primary_key=True)
    po_number = Column(String, unique=True, nullable=False)
    project_id = Column(Integer, ForeignKey('projects.id'))
    vendor_id = Column(Integer, ForeignKey('vendors.id'))
    state = Column(Enum(POState), default=POState.PENDING)
    amount = Column(Float)
    description = Column(Text)
    status = Column(Text)

    project = relationship('Project', back_populates='pos')
    vendor = relationship('Vendor', back_populates='pos', foreign_keys=[vendor_id])
    invoices = relationship('Invoice', back_populates='po')
    receipts = relationship('Receipt', back_populates='po')
    bills = relationship('Bill', back_populates='po')
    transactions = relationship('Transaction', back_populates='po')
    spend_money_transactions = relationship(
        'SpendMoneyTransaction', back_populates='po')
    actuals = relationship('Actual', back_populates='po')


# Vendor Model
class Vendor(Base):
    __tablename__ = 'vendors'

    id = Column(Integer, primary_key=True)
    vendor_name = Column(String, unique=True, nullable=False)
    contact_id = Column(Integer, ForeignKey('contacts.id'))

    pos = relationship('PO', back_populates='vendor')
    contact = relationship(
        'Contact',
        back_populates='vendor',
        foreign_keys=[contact_id]
    )
    tax_form = relationship(
        'TaxForm',
        back_populates='vendor',
        uselist=False  # Indicates one-to-one relationship
    )


# TaxForm Model
class TaxForm(Base):
    __tablename__ = 'tax_forms'

    id = Column(Integer, primary_key=True)
    vendor_id = Column(Integer, ForeignKey('vendors.id'))
    form_type = Column(String)  # e.g., 'W-9', 'W-8BEN'
    status = Column(String)     # e.g., 'Pending', 'Validated'
    data = Column(Text)         # JSON or text data extracted from the form

    vendor = relationship(
        'Vendor',
        back_populates='tax_form',
        foreign_keys=[vendor_id]
    )


# Contact Model
class Contact(Base):
    __tablename__ = 'contacts'

    id = Column(Integer, primary_key=True)
    contact_id = Column(String, unique=True)
    name = Column(String)
    email = Column(String)
    phone = Column(String)

    vendor = relationship(
        'Vendor',
        back_populates='contact',
        uselist=False  # One-to-one relationship
    )


# Invoice Model
class Invoice(Base):
    __tablename__ = 'invoices'

    id = Column(Integer, primary_key=True)
    po_id = Column(Integer, ForeignKey('pos.id'))
    file_path = Column(String, nullable=False)
    data = Column(Text)         # JSON or text data extracted from the invoice
    status = Column(String)     # e.g., 'Pending', 'Validated'

    po = relationship('PO', back_populates='invoices')


# Receipt Model
class Receipt(Base):
    __tablename__ = 'receipts'

    id = Column(Integer, primary_key=True)
    po_id = Column(Integer, ForeignKey('pos.id'))
    file_path = Column(String, nullable=False)
    data = Column(Text)         # JSON or text data extracted from the receipt
    status = Column(String)     # e.g., 'Pending', 'Validated'

    po = relationship('PO', back_populates='receipts')


# Bill Model
class Bill(Base):
    __tablename__ = 'bills'

    id = Column(Integer, primary_key=True)
    po_id = Column(Integer, ForeignKey('pos.id'))
    bill_id = Column(String, unique=True)
    state = Column(Enum(BillState), default=BillState.DRAFT)
    amount = Column(Float)
    due_date = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)

    po = relationship('PO', back_populates='bills')


# Transaction Model (Mercury Bank Transactions)
class Transaction(Base):
    __tablename__ = 'transactions'

    id = Column(Integer, primary_key=True)
    po_id = Column(Integer, ForeignKey('pos.id'))
    transaction_id = Column(String, unique=True)
    state = Column(Enum(TransactionState), default=TransactionState.PENDING)
    amount = Column(Float)
    executed_at = Column(DateTime)

    po = relationship('PO', back_populates='transactions')


# SpendMoneyTransaction Model (Credit Card and Petty Cash)
class SpendMoneyTransaction(Base):
    __tablename__ = 'spend_money_transactions'

    id = Column(Integer, primary_key=True)
    transaction_id = Column(String, unique=True, nullable=False)
    po_id = Column(Integer, ForeignKey('pos.id'), nullable=False)
    state = Column(Enum(SpendMoneyState), default=SpendMoneyState.DRAFT)
    amount = Column(Float, nullable=False)
    description = Column(Text)  # Added description field
    created_at = Column(DateTime, default=datetime.utcnow)

    po = relationship('PO', back_populates='spend_money_transactions')


# Actual Model
class Actual(Base):
    __tablename__ = 'actuals'

    id = Column(Integer, primary_key=True)
    po_id = Column(Integer, ForeignKey('pos.id'))
    amount = Column(Float)
    description = Column(Text)
    date = Column(DateTime)
    source = Column(String)  # e.g., 'Xero', 'Mercury', 'Manual'

    po = relationship('PO', back_populates='actuals')


# MainItem Model
class MainItem(Base):
    __tablename__ = 'main_items'
    id = Column(Integer, primary_key=True, autoincrement=True)
    item_id = Column(String, unique=True)
    name = Column(String)
    project_id = Column(String)
    numbers = Column(String)
    description = Column(Text)
    tax_form = Column(String)
    folder = Column(String)
    amount = Column(String)
    po_status = Column(String)
    producer_pm = Column(String)
    updated_date = Column(String)

    sub_items = relationship(
        'SubItem',
        back_populates='main_item',
        cascade='all, delete-orphan'
    )


# SubItem Model
class SubItem(Base):
    __tablename__ = 'sub_items'
    id = Column(Integer, primary_key=True, autoincrement=True)
    subitem_id = Column(String, unique=True)
    main_item_id = Column(String, ForeignKey('main_items.item_id'))
    status = Column(String)
    invoice_number = Column(String)
    description = Column(Text)
    amount = Column(Float)
    quantity = Column(Float)
    account_number = Column(String)
    invoice_date = Column(String)
    link = Column(String)
    due_date = Column(String)
    creation_log = Column(Text)

    main_item = relationship('MainItem', back_populates='sub_items')

