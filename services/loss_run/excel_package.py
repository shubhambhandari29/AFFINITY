"""Normalize known openpyxl OOXML serialization issues in loss-run exports."""

from io import BytesIO
from xml.etree import ElementTree as ET
from zipfile import ZipFile

SPREADSHEET_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
RELATIONSHIP_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
# Excel's font child sequence (MS-OE376, section 2.1.733).
FONT_ORDER = (
    "b",
    "i",
    "strike",
    "condense",
    "extend",
    "outline",
    "shadow",
    "u",
    "vertAlign",
    "sz",
    "color",
    "name",
    "family",
    "charset",
    "scheme",
)


def save_report_workbook(workbook) -> bytes:
    """Save without changing cells, pivots, cache relationships or image data.

    Normalization must run AFTER save: openpyxl adds the invalid pivot r:id
    during serialization, so clearing pivot.id before save does not fix it.
    """
    saved = BytesIO()
    workbook.save(saved)
    output = BytesIO()
    ns = f"{{{SPREADSHEET_NS}}}"
    with ZipFile(saved) as source, ZipFile(output, "w") as target:
        for part in source.infolist():
            data = source.read(part.filename)
            if part.filename.startswith(
                "xl/pivotTables/pivotTable"
            ) and part.filename.endswith(".xml"):
                root = ET.fromstring(data)
                # Pivot definitions link by cacheId and their .rels part, NOT r:id.
                # r:id on pivotCacheDefinition is valid and must be retained.
                root.attrib.pop(f"{{{RELATIONSHIP_NS}}}id", None)
                data = ET.tostring(root, encoding="utf-8", xml_declaration=True)
            elif part.filename == "xl/styles.xml":
                root = ET.fromstring(data)
                order = {ns + name: index for index, name in enumerate(FONT_ORDER)}
                for font in root.iter(ns + "font"):
                    font[:] = sorted(
                        font, key=lambda item: order.get(item.tag, len(order))
                    )
                # Converted templates may reference named styles that openpyxl
                # drops. Keep each cell's explicit formatting, using Normal as
                # the base only where the serialized base-style index is invalid.
                bases = root.find(ns + "cellStyleXfs")
                cells = root.find(ns + "cellXfs")
                if bases is not None and cells is not None:
                    for style in cells:
                        if int(style.get("xfId", "0")) >= len(bases):
                            style.set("xfId", "0")
                data = ET.tostring(root, encoding="utf-8", xml_declaration=True)
            target.writestr(part, data)
    return output.getvalue()
