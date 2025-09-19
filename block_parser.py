import re


class BlockStringParser:
    """
    Parses a Block.toString() dump like:
    Block{rows=3, col1=[a,b,c], col2=[1,2,3]}
    into row-wise records.
    """

    def __init__(self, block_str: str):
        self.block_str = block_str.strip()
        self.rows = 0
        self.columns = {}
        self._parse()

    def _parse(self):
        # Extract row count
        rows_match = re.search(r"rows=(\d+)", self.block_str)
        if not rows_match:
            raise ValueError("Could not find row count in block string")
        self.rows = int(rows_match.group(1))
        # Extract all columns of form colName=[...]
        col_pattern = re.compile(r"(\w+)=\[(.*?)\]")
        for col, values_str in col_pattern.findall(self.block_str):
            # Split values by comma, keeping "null" as None
            values = [v.strip() for v in values_str.split(",")]
            values = [None if v.lower() == "null" else v for v in values]
            self.columns[col] = values

    def get_row_strings(self):
        row_strings = []
        for i in range(self.rows):
            parts = []
            for col, values in self.columns.items():
                val = values[i] if i < len(values) else None
                parts.append(f"[{col} : {val}]")
            row_strings.append(", ".join(parts))
        return row_strings


def process_block_file(input_file: str, output_file: str):
    with open(input_file, "r") as f:
        block_str = f.read()
    parser = BlockStringParser(block_str)
    rows = parser.get_row_strings()
    with open(output_file, "w") as out:
        for row in rows:
            out.write(row + "\n")
    return rows


def write_diff(main_rows, updated_rows, diff_file: str):
    max_len = max(len(main_rows), len(updated_rows))
    with open(diff_file, "w") as out:
        for i in range(max_len):
            main_val = main_rows[i] if i < len(main_rows) else "<no row>"
            upd_val = updated_rows[i] if i < len(updated_rows) else "<no row>"
            if main_val != upd_val:
                out.write(f"Row {i}:\n")
                out.write(f"  Mainline: {main_val}\n")
                out.write(f"  Updated : {upd_val}\n\n")


if __name__ == "__main__":
    mainline_file = "mainline_block.txt"
    updated_file = "updated_block.txt"
    mainline_rows = process_block_file(mainline_file, "mainline_records.txt")
    updated_rows = process_block_file(updated_file, "updated_records.txt")
    write_diff(mainline_rows, updated_rows, "diff_records.txt")
    print("Records written to mainline_records.txt, updated_records.txt, and diff_records.txt")
