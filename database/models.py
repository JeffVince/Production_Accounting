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
    Table, Double,
)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from database.base import Base
import logging

logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.ERROR)
logging.getLogger("sqlalchemy.pool").setLevel(logging.ERROR)

# Association Table for DetailItem and BillLineItem
detail_items_has_bill_line_items = Table(
    "detail_items_has_bill_line_items",
    Base.metadata,
    Column(
        "detail_items_detail_item_id",
        Integer,
        ForeignKey("detail_items.detail_item_id"),
        primary_key=True,
    ),
    Column(
        "bill_line_items_bill_line_item_id",
        Integer,
        ForeignKey("bill_line_items.bill_line_item_id"),
        primary_key=True,
    ),
)


# Contact Model
class Contact(Base):
    __tablename__ = "contacts"

    contact_id = Column(Integer, primary_key=True, autoincrement=True)
    pulse_id = Column(Integer, nullable=True)
    name = Column(String(100), nullable=False)
    vendor_type = Column(String(45), nullable=True)
    payment_details = Column(String(255), nullable=False, default="PENDING")
    email = Column(String(100), nullable=True)
    phone = Column(String(45), nullable=True)
    address_line_1 = Column(String(255), nullable=True)
    city = Column(String(100), nullable=True)
    zip = Column(String(20), nullable=True)
    tax_ID = Column(String(45), unique=True, nullable=True)
    tax_form_link = Column(String(255), nullable=True)
    tax_form_status = Column(String(45), nullable=False, default="PENDING")

    # Relationships
    purchase_orders = relationship("PurchaseOrder", back_populates="contact")


# Project Model
class Project(Base):
    __tablename__ = "projects"

    project_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    status = Column(String(45), nullable=False)

    purchase_orders = relationship("PurchaseOrder", back_populates="project")


# POState Model
class POState(Base):
    __tablename__ = "po_states"

    state = Column(String(45), primary_key=True)
    description = Column(String(255), nullable=True)

    purchase_orders = relationship("PurchaseOrder", back_populates="po_state")


# PurchaseOrder Model
class PurchaseOrder(Base):
    __tablename__ = "purchase_orders"

    pulse_id = Column(Double, primary_key=True, nullable=False, comment='Integration with Monday.com')
    po_number = Column(Integer, unique=True, nullable=False)
    project_id = Column(Integer, ForeignKey("projects.project_id"), nullable=False)
    contact_id = Column(Integer, ForeignKey("contacts.contact_id"), nullable=True)
    state = Column(String(45), ForeignKey("po_states.state"), nullable=False, default="PENDING")
    amount_total = Column(DECIMAL(15, 2), nullable=False, default=0.00)
    description = Column(String(255), nullable=True)
    folder_link = Column(String(255), nullable=True)
    producer = Column(String(100), nullable=True)
    po_type = Column(String(45), nullable=False)
    issue_type = Column(String(45), nullable=True)
    tax_form_link = Column(String(255), nullable=True)

    # Relationships
    project = relationship("Project", back_populates="purchase_orders")
    contact = relationship("Contact", back_populates="purchase_orders")
    po_state = relationship("POState", back_populates="purchase_orders")
    bills = relationship("Bill", back_populates="purchase_order")
    detail_items = relationship("DetailItem", back_populates="purchase_order")
    spend_money_transactions = relationship("SpendMoney", back_populates="purchase_order")


# DetailItem Model
class DetailItem(Base):
    __tablename__ = "detail_items"

    pulse_id = Column(Double, primary_key=True, nullable=False, comment='Integration with Monday.com')
    detail_item_id = Column(Integer, nullable=False, default=1)  # Not a primary key
    transaction_date = Column(DateTime, nullable=True)
    due_date = Column(DateTime, nullable=True)
    rate = Column(DECIMAL(15, 2), nullable=False)
    quantity = Column(DECIMAL(15, 2), nullable=False, default=1.00)
    #sub_total = Column(DECIMAL(15, 2), nullable=False)
    description = Column(String(255), nullable=True)
    file_link = Column(String(255), nullable=True, comment='Link to invoice or receipt in Dropbox')
    is_receipt = Column(Boolean, nullable=False, default=False, comment='1 for receipts, 0 for invoices')
    state = Column(String(45), ForeignKey("detail_item_states.state"), nullable=False, default="PENDING")
    issue_type = Column(String(45), nullable=True)
    account_number = Column(Integer, nullable=True)
    parent_id = Column(Double, ForeignKey("purchase_orders.pulse_id"), nullable=False)

    # Relationships
    purchase_order = relationship("PurchaseOrder", back_populates="detail_items")
    detail_item_state = relationship("DetailItemState", back_populates="detail_items")
    spend_money = relationship("SpendMoney", back_populates="detail_item", uselist=False)
    bill_line_items = relationship(
        "BillLineItem",
        secondary=detail_items_has_bill_line_items,
        back_populates="detail_items",
    )


# XeroBillState Model
class XeroBillState(Base):
    __tablename__ = "xero_bill_states"

    state = Column(String(45), primary_key=True)
    description = Column(String(255), nullable=True)

    bills = relationship("Bill", back_populates="bill_state")


# Bill Model
class Bill(Base):
    __tablename__ = "bills"

    bill_id = Column(Integer, primary_key=True, autoincrement=True)
    po_id = Column(Double, ForeignKey("purchase_orders.pulse_id"), nullable=False)
    xero_bill_id = Column(String(100), nullable=True)
    state = Column(String(45), ForeignKey("xero_bill_states.state"), nullable=False, default="Draft")
    due_date = Column(Date, nullable=True)
    issue_date = Column(Date, nullable=True)
    transaction_date = Column(Date, nullable=True)
    contracted_total = Column(DECIMAL(15, 2), nullable=False, default=0.00)
    file_link = Column(String(255), nullable=True)

    # Relationships
    purchase_order = relationship("PurchaseOrder", back_populates="bills")
    bill_state = relationship("XeroBillState", back_populates="bills")
    bill_line_items = relationship("BillLineItem", back_populates="bill")
    bank_transactions = relationship("BankTransaction", back_populates="bill")


# DetailItemState Model
class DetailItemState(Base):
    __tablename__ = "detail_item_states"

    state = Column(String(45), primary_key=True)
    description = Column(String(255), nullable=True)

    detail_items = relationship("DetailItem", back_populates="detail_item_state")


# XeroSpendMoneyState Model
class XeroSpendMoneyState(Base):
    __tablename__ = "xero_spend_money_states"

    state = Column(String(45), primary_key=True)
    description = Column(String(255), nullable=True)

    spend_money_transactions = relationship("SpendMoney", back_populates="spend_money_state")


# SpendMoney Model
class SpendMoney(Base):
    __tablename__ = "spend_money"

    spend_money_id = Column(Integer, primary_key=True, autoincrement=True)
    po_id = Column(Double, ForeignKey("purchase_orders.pulse_id"), nullable=False)
    xero_spend_money_id = Column(String(100), nullable=True)
    detail_item_id = Column(Integer, ForeignKey("detail_items.detail_item_id"), nullable=False)
    amount = Column(DECIMAL(15, 2), nullable=False)
    tax_account = Column(Integer, nullable=True)
    file_link = Column(String(255), nullable=True)
    state = Column(String(45), ForeignKey("xero_spend_money_states.state"), nullable=False, default="Draft")

    # Relationships
    purchase_order = relationship("PurchaseOrder", back_populates="spend_money_transactions")
    detail_item = relationship("DetailItem", back_populates="spend_money", uselist=False)
    spend_money_state = relationship("XeroSpendMoneyState", back_populates="spend_money_transactions")
    bank_transactions = relationship("BankTransaction", back_populates="spend_money")


# MercuryState Model
class MercuryState(Base):
    __tablename__ = "mercury_states"

    state = Column(String(45), primary_key=True)
    description = Column(String(255), nullable=True)

    bank_transactions = relationship("BankTransaction", back_populates="mercury_state")


# BankTransaction Model
class BankTransaction(Base):
    __tablename__ = "bank_transactions"

    bank_transaction_id = Column(Integer, primary_key=True, autoincrement=True)
    recipient = Column(String(100), nullable=True)
    amount = Column(DECIMAL(15, 2), nullable=False)
    bill_id = Column(Integer, ForeignKey("bills.bill_id"), nullable=True)
    spend_money_id = Column(Integer, ForeignKey("spend_money.spend_money_id"), nullable=True)
    mercury_transaction_id = Column(String(100), nullable=True)
    state = Column(String(45), ForeignKey("mercury_states.state"), nullable=False, default="Pending")

    # Relationships
    bill = relationship("Bill", back_populates="bank_transactions")
    spend_money = relationship("SpendMoney", back_populates="bank_transactions")
    mercury_state = relationship("MercuryState", back_populates="bank_transactions")


# BillLineItem Model
class BillLineItem(Base):
    __tablename__ = "bill_line_items"

    bill_line_item_id = Column(Integer, primary_key=True, autoincrement=True)
    bill_id = Column(Integer, ForeignKey("bills.bill_id"), nullable=False)
    description = Column(String(255), nullable=True)
    rate = Column(DECIMAL(15, 2), nullable=False)
    quantity = Column(DECIMAL(15, 2), nullable=False, default=1.00)
    sub_total = Column(DECIMAL(15, 2), nullable=False)
    tax_account = Column(Integer, nullable=True)

    # Relationships
    bill = relationship("Bill", back_populates="bill_line_items")
    detail_items = relationship(
        "DetailItem",
        secondary=detail_items_has_bill_line_items,
        back_populates="bill_line_items",
    )


