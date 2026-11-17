import pymupdf
import io
import magic
class PDFValidator:
    """Validates raw PDF bytes with four checks: encryption, %%EOF marker, page
    count, and file size. Each check returns (is_valid, check_dict) where check_dict
    has keys type/isvalid/error.
    """

    def end_of_file_validation(self, content):
        """Check that the binary content contains the %%EOF marker.

        Returns:
            tuple: (is_valid, {'type': 'END OF FILE', 'isvalid': bool, 'error': str}).
        """
        try:
            if b'%%EOF' in content:
                return True, {"type": "END OF FILE","isvalid": True, "error": ""}
            return False, {"type": "END OF FILE","isvalid": False, "error": "END OF FILE NOT FOUND"}

        except Exception as e:
            return False, {"type": "FILE OPEN VALIDATION","isvalid": False, "error": str(e)}
        
    def zero_pages_file_open_validation(self, content):
        """Open the content with PyMuPDF and check the document has at least one page.

        Returns:
            tuple: (is_valid, {'type': 'FILE OPEN VALIDATION', 'isvalid': bool, 'error': str}).
        """
        try:
            with pymupdf.open(stream=io.BytesIO(content)) as docs:
                if docs.pages == 0 or docs.pages is None or str(docs.pages) == "":
                    return False, {"type": "FILE OPEN VALIDATION","isvalid": False, "error": "ZERO PAGES"}

                return True, {"type": "FILE OPEN VALIDATION","isvalid": True, "error": ""}

        except Exception as e:
            return False, {"type": "FILE OPEN VALIDATION","isvalid": False, "error": str(e)}

    def is_encrypted(self, content):
        """Check with PyMuPDF whether the document is encrypted.

        Returns:
            tuple: (is_valid, {'type': 'ENCRYPTION VALIDATION', 'isvalid': bool, 'error': str}).
        """
        try:
            with pymupdf.open(stream=io.BytesIO(content)) as docs:
                if docs.is_encrypted:
                    return False, {"type": "ENCRYPTION VALIDATION","isvalid": False, "error": "FILE IS ENCRYPTED"}

                return True, {"type": "ENCRYPTION VALIDATION","isvalid": True, "error": ""}

        except Exception as e:
            return False, {"type": "ENCRYPTION VALIDATION","isvalid": False, "error": str(e)}

    def file_size_validation(self, length):
        """Check that the file length in bytes is greater than zero.

        Returns:
            tuple: (is_valid, {'type': 'FILE SIZE VALIDATION', 'isvalid': bool, 'error': str}).
        """
        try: 
            if length > 0:
                return True, {"type": "FILE SIZE VALIDATION","isvalid": True, "error": ""}

            return False, {"type": "FILE SIZE VALIDATION","isvalid": False, "error": "ZERO SIZE FILE"}
        
        except Exception as e:
            return False, {"type": "FILE OPEN VALIDATION","isvalid": False, "error": str(e)}




    def validate_pdf(self, content, length):
        """Run all PDF checks (encryption, %%EOF, page count, size) for one document.

        Returns:
            dict: {'isvalid': bool (True only if every check passes),
                   'validation_checks': list of per-check dicts}.
        """

        validations = [
            self.is_encrypted(content),
            self.end_of_file_validation(content),
            self.zero_pages_file_open_validation(content),
            self.file_size_validation(length)
        ]

        isvalid = all(result[0] for result in validations)

        validation_checks = [
            result[1] for result in validations
        ]

        return {
            "isvalid": isvalid,
            "validation_checks": validation_checks
        }


class Validator:
    """Batch validator for mapInPandas: detects each file's MIME type and, for
    PDFs, runs the full PDFValidator check suite; adds isvalid, validation_checks,
    and mime_type columns to each pandas batch.
    """

    def __init__(self):
        """Create the validator with an internal PDFValidator instance."""
        self.pdf_validator = PDFValidator()

    def get_mime_type(self, content):
        """Detect the MIME type of the content using python-magic.

        Returns:
            str: the MIME type, or 'ERROR:<message>' if detection fails.
        """
        try:            
            return magic.from_buffer(content, mime=True)
        except Exception as e:
            return "ERROR:" + str(e)

    def validate_batch(self, iterator):
        """mapInPandas batch function: yield each pandas batch with validation columns.

        For each row: non-PDF MIME types or detection failures are marked invalid;
        PDFs are validated via PDFValidator. Adds isvalid (bool), validation_checks
        (list of check dicts), and mime_type columns to the batch.
        """

        for batch in iterator:

            isvalid = []
            error = []
            mime_type = []

            for _, row in batch.iterrows():

                mime = self.get_mime_type(row["content"])
                if mime.startswith("ERROR:"):
                    isvalid_flag = False
                    isvalid.append(isvalid_flag)
                    error.append([{"type": "FILE OPEN VALIDATION","isvalid": False, "error": "FAILED TO IDENTIFY MIME TYPE"}])
                    mime_type.append(mime)

                elif mime == "application/pdf":
                    validation = self.pdf_validator.validate_pdf(
                        row["content"],
                        row["length"]
                    )

                    isvalid_flag = validation["isvalid"]
                    isvalid.append(isvalid_flag)
                    error.append(validation["validation_checks"])
                    mime_type.append(mime)
                else:
                    isvalid_flag = False
                    isvalid.append(isvalid_flag)
                    error.append([{"type": "FILE OPEN VALIDATION","isvalid": False, "error": "MIME TYPE NOT SUPPORTED"}])
                    mime_type.append(mime)

            batch["isvalid"] = isvalid
            batch["validation_checks"] = error
            batch["mime_type"] = mime_type

            yield batch


                