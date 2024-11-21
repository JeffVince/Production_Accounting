# database/dropbox_repository.py

from sqlalchemy.exc import SQLAlchemyError
from database.models import Invoice, Receipt, PO
from database.db_util import get_db_session
import logging

logger = logging.getLogger(__name__)

def add_file_record(file_data):
    """
    Adds a file record (invoice or receipt) to the database.
    """
    try:
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=file_data['po_number']).first()
            if not po:
                logger.warning(f"PO {file_data['po_number']} not found")
                return None

            if file_data['file_type'] == 'invoice':
                invoice = Invoice(
                    po_id=po.id,
                    file_path=file_data['file_path'],
                    data=file_data.get('data', ''),
                    status=file_data.get('status', 'Pending')
                )
                session.add(invoice)
                logger.info(f"Added invoice for PO {po.po_number}")
                return invoice
            elif file_data['file_type'] == 'receipt':
                receipt = Receipt(
                    po_id=po.id,
                    file_path=file_data['file_path'],
                    data=file_data.get('data', ''),
                    status=file_data.get('status', 'Pending')
                )
                session.add(receipt)
                logger.info(f"Added receipt for PO {po.po_number}")
                return receipt
            else:
                logger.error(f"Unknown file type: {file_data['file_type']}")
                return None
    except SQLAlchemyError as e:
        logger.error(f"Error adding file record: {e}")
        raise e

def get_files_by_po(po_number):
    """
    Retrieves invoices and receipts associated with a PO.
    """
    try:
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if not po:
                logger.warning(f"PO {po_number} not found")
                return {'invoices': [], 'receipts': []}

            invoices = session.query(Invoice).filter_by(po_id=po.id).all()
            receipts = session.query(Receipt).filter_by(po_id=po.id).all()
            return {'invoices': invoices, 'receipts': receipts}
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving files for PO {po_number}: {e}")
        raise e

def update_file_status(file_id, file_type, new_status):
    """
    Updates the status of an invoice or receipt.
    """
    try:
        with get_db_session() as session:
            if file_type == 'invoice':
                file_record = session.query(Invoice).filter_by(id=file_id).first()
            elif file_type == 'receipt':
                file_record = session.query(Receipt).filter_by(id=file_id).first()
            else:
                logger.error(f"Unknown file type: {file_type}")
                return False

            if not file_record:
                logger.warning(f"{file_type.capitalize()} with ID {file_id} not found")
                return False

            file_record.status = new_status
            session.commit()
            logger.info(f"Updated {file_type} ID {file_id} to status {new_status}")
            return True
    except SQLAlchemyError as e:
        logger.error(f"Error updating file status: {e}")
        raise e