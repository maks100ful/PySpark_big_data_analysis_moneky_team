import csv

def pretty_print_tsv(file_path, max_rows=20):
    with open(file_path, newline='', encoding='utf-8') as tsvfile:
        reader = csv.reader(tsvfile, delimiter='\t')
        rows = list(reader)

    if not rows:
        print("Empty file.")
        return

    num_cols = len(rows[0])
    col_widths = [0] * num_cols

    for row in rows[:max_rows] + [rows[0]]: 
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    for row in rows[:max_rows]:
        pretty_row = "  ".join(cell.ljust(col_widths[i]) for i, cell in enumerate(row))
        print(pretty_row)

    if len(rows) > max_rows:
        print(f"\n...and {len(rows) - max_rows} more rows.")
        
pretty_print_tsv("raw_data/title.basics.tsv")
