import binary_reader.sqlite3_schema as sqlite3

class SQLiteFile:
    def __init__(self, file):
        self.file = file
        self.pages = {}
        self.load()

    def load(self):
        self.file.seek(0)
        self.config = self.readbin(sqlite3.header_schema, self.file.read(100))
        self.tables = self.load_btree(0)

    def load_btree(self, index):
        if isinstance(index, str):
            for _, v in self.tables:
                if v[1] == index:
                    print("found", v)
                    return self.load_btree(v[3]-1)
            return []
        page = self.load_btree_page(index)
        rows = []
        page_type = page.header['page_type']
        if page_type == 13:
            for c, p in zip(page.cells, page.payloads):
                rows.append((c['rowid'], p['column_contents']))
        elif page_type == 5:
            for c in page.cells:
                print("load btree page %s -> %s, %s" % (index+1, c['left_child_page'], c['rowid']))
                rows += self.load_btree(c['left_child_page']-1)
            print("load btree page %s -> %s, -1" % (index+1, page.header['right_most_page']))
            rows += self.load_btree(page.header['right_most_page']-1)
        else:
            print("unknown page type %s" % page_type)
        return rows

    def load_btree_page(self, index):
        if index in self.pages:
            return self.pages[index]
        self.file.seek(index * self.config['page_size'])
        self.pages[index] = Page(self.config, self.file.read(self.config['page_size']), index, file=self.file)
        return self.pages[index]

    @staticmethod
    def readbin(schema, bin_, *, cache=None):
        if cache is None:
            cache = {}
        return dict(sqlite3.read_schema_list(schema, bin_, table=sqlite3.format_table, cache=cache))


class Page:
    def __init__(self, config, page_bin, index, *, file=None):
        self.config = config
        self.page_bin = page_bin
        self.index = index
        self.load(index == 0)
        self.load_cells()
        self.load_cells_payload(file)

    def load(self, first=False):
        self.header = SQLiteFile.readbin(sqlite3.page_header_schema, self.page_bin[100:] if first else self.page_bin)
        return self

    def load_cells(self):
        self.cells = []
        cache = self.header.copy()
        cache['config'] = self.config
        for i in self.header['cell_offset_array']:
            self.cells.append(SQLiteFile.readbin(sqlite3.cell_header_schema, self.page_bin[i:], cache=cache))
        return self

    def load_overflows(self, file, cell=None):
        if cell is None:
            for i in self.cells:
                self.load_overflows(file, i)
        elif cell['payload_size'] > cell['local_payload_size']:
            cell['overflow_pages'], cell['full_payload'] = self.load_overflow(file, cell['payload_size'],
                                                                              cell['overflow_page'] - 1,
                                                                              size=cell['local_payload_size'],
                                                                              acc=[cell['payload']])

    def load_overflow(self, file, total_size, acc_page, size=0, acc=None):
        if acc is None:
            acc = []
        if not isinstance(acc_page, list):
            acc_page = [acc_page]
        if acc_page[-1] == 0:
            return acc_page, acc
        file.seek(acc_page[-1] * self.config['page_size'])
        page_bin = file.read(self.config['page_size'])
        page_header = SQLiteFile.readbin(sqlite3.page_overflow_header_schema, page_bin)
        # print("load overflow %s -> %s" % (acc_page, page_header))
        offset = page_header['offset']
        content = page_bin[offset:max(offset + total_size - size, 0)]
        acc.append(content)
        acc_page.append(page_header['next_page'] - 1)
        size += len(content)
        if size >= total_size:
            return acc_page, acc
        return self.load_overflow(file, total_size, acc_page, acc)

    def load_cells_payload(self, file=None):
        self.payloads = []
        for i in self.cells:
            if 'payload' not in i:
                self.payloads.append(None)
                continue
            payload = i['payload']
            if i['payload_size'] > i['local_payload_size']:
                if 'full_payload' not in i and file is not None:
                    self.load_overflows(file, i)
                if 'full_payload' in i:
                    payload = b''.join(i['full_payload'])
                else:
                    print("overflow not loaded %s" % i)
            self.payloads.append(SQLiteFile.readbin(sqlite3.record_format_schema, payload))
        return self


def _init_test():
    import sqlite3
    conn = sqlite3.connect('example.db')
    c = conn.cursor()

    # Create table
    c.execute('''CREATE TABLE stocks
                 (date text, trans text, symbol text, qty real, price real)''')

    # Insert a row of data
    for i in range(1000):
        c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT%s',100,10.14)" % ("142857" * i + "!"))

    # Save (commit) the changes
    conn.commit()

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()

def _test():
    ""
    ##
    # create_test_db()
    file = SQLiteFile(open("example.db", "rb"))
    ##
    file.load_btree(0)
    ##
    rows = file.load_btree("stocks")
    ##
    import csv
    with open("stocks.csv", "w", encoding="utf-8") as fout:
        writer = csv.writer(fout, delimiter=",", lineterminator="\n", quotechar='"', doublequote=True, strict=True)
        for _, v in sorted(rows):
            writer.writerow(v)
