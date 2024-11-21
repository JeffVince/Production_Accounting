from tests.base_test import BaseTestCase
from services.tax_form_service import TaxFormService
from database.models import PO, Vendor


class MockOCRService:
    def extract_text_from_w9(self, tax_form_data):
        return "Name: Vendor\nTax ID: 123456789\nSignature: Yes"

class TestTaxFormService(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.service = TaxFormService()
        self.service.ocr_service = MockOCRService()

        # Add test data to the database
        with self.session_scope() as session:
            vendor = Vendor(vendor_name='Vendor A')
            po = PO(po_number='PO123', vendor=vendor)
            session.add(po)

    def test_process_tax_form_valid(self):
        tax_form_data = b'fake_pdf_data'
        valid = self.service.process_tax_form('PO123', tax_form_data)
        self.assertTrue(valid)
        with self.session_scope() as session:
            po = session.query(PO).filter_by(po_number='PO123').first()
            self.assertEqual(po.vendor.tax_form.status, 'Validated')

    def test_validate_tax_form_invalid(self):
        self.service.ocr_service.extract_text_from_w9 = lambda x: "Incomplete Data"
        valid = self.service.validate_tax_form("Incomplete Data")
        self.assertFalse(valid)