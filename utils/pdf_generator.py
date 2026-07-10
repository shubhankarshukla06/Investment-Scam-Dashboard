from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Image, Spacer, Paragraph, FrameBreak
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_RIGHT
from datetime import datetime, timedelta, timezone
import os

IST = timezone(timedelta(hours=5, minutes=30))


def generate_pdf(source_url: str, image_paths: list[str], output_path: str) -> int:
    PAGE_W, PAGE_H = A4
    MARGIN = 1.5 * cm
    CONTENT_W = PAGE_W - 2 * MARGIN

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=MARGIN,
        bottomMargin=MARGIN,
        title="Case Report",
        author="Scam Intelligence Unit",
    )

    styles = getSampleStyleSheet()

    ts_style = ParagraphStyle(
        "Timestamp",
        parent=styles["Normal"],
        fontSize=9,
        textColor=colors.HexColor("#333333"),
        alignment=TA_RIGHT,
        spaceAfter=4,
    )
    url_top_style = ParagraphStyle(
        "UrlTop",
        parent=styles["Normal"],
        fontSize=10,
        textColor=colors.HexColor("#cc0000"),
        alignment=TA_CENTER,
        spaceAfter=10,
        fontName="Helvetica-Bold",
    )
    url_bottom_style = ParagraphStyle(
        "UrlBottom",
        parent=styles["Normal"],
        fontSize=10,
        textColor=colors.HexColor("#333333"),
        alignment=TA_CENTER,
        spaceBefore=10,
        fontName="Helvetica-Bold",
    )

    now_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    HEADER_H = 1.6 * cm
    FOOTER_H = 1.0 * cm
    GAP      = 0.6 * cm   # small gap between SS1 and SS2
    AVAIL_H  = PAGE_H - 2 * MARGIN - HEADER_H - FOOTER_H - GAP
    IMG_H    = AVAIL_H / 2
    IMG_W    = CONTENT_W

    story = []

    # Chunk images into pairs
    pairs = [image_paths[i:i+2] for i in range(0, len(image_paths), 2)]

    for idx, pair in enumerate(pairs):
        # ── Timestamp top-right ──────────────────────────────────────────
        story.append(Paragraph(now_str, ts_style))

        # ── Source URL centered, red ─────────────────────────────────────
        story.append(Paragraph(source_url, url_top_style))

        # ── Screenshot 1 ────────────────────────────────────────────────
        if len(pair) >= 1:
            try:
                story.append(Image(pair[0], width=IMG_W, height=IMG_H, kind="bound"))
            except Exception:
                story.append(Paragraph("(image error)", styles["Normal"]))

        # ── Small gap ───────────────────────────────────────────────────
        story.append(Spacer(1, GAP))

        # ── Screenshot 2 ────────────────────────────────────────────────
        if len(pair) >= 2:
            try:
                story.append(Image(pair[1], width=IMG_W, height=IMG_H, kind="bound"))
            except Exception:
                story.append(Paragraph("(image error)", styles["Normal"]))

        # ── Source URL bottom center ─────────────────────────────────────
        story.append(Paragraph(source_url, url_bottom_style))

        # ── Page break between pairs (not after last) ────────────────────
        if idx < len(pairs) - 1:
            from reportlab.platypus import PageBreak
            story.append(PageBreak())

    doc.build(story)

    # Count pages
    try:
        from pypdf import PdfReader
        return len(PdfReader(output_path).pages)
    except Exception:
        return len(pairs)
