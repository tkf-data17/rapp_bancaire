import fitz

pdf_path = "ocr_split_pages/ocr_page_7.pdf"
doc = fitz.open(pdf_path)
page = doc[0]
words = page.get_text("words")

# Group by block/line to mimic extract_table.py
rows = {}
for w in words:
    key = (w[5], w[6])
    if key not in rows: rows[key] = []
    rows[key].append(w)

sorted_keys = sorted(rows.keys())
for key in sorted_keys:
    line_words = sorted(rows[key], key=lambda x: x[0])
    text = " ".join([w[4] for w in line_words])
    
    if key[0] == 10:
        print(f"Key={key}: {text}")
