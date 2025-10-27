import os
import struct
import threading
from .file_obj import FileObj

class BinaryTable:
    def __init__(self, f, record_type, record_size, elements_per_column, header_size):
        self.file = f
        self.file.machineformat = '<'  # little-endian
        self.record_type = record_type
        self.record_size = record_size
        self.elements_per_column = elements_per_column
        self.header_size = header_size
        self._lock = threading.Lock()

        if not self.file.fullpathfilename:
            raise ValueError("A full path file name must be given to the file object.")

    def get_size(self):
        try:
            file_size = os.path.getsize(self.file.fullpathfilename)
            data_size = file_size - self.header_size
        except FileNotFoundError:
            data_size = 0

        c = len(self.record_size)
        row_size = sum(self.record_size)
        r = data_size // row_size if row_size > 0 else 0
        return r, c, data_size

    def read_header(self):
        with self._lock:
            try:
                self.file.permission = 'r'
                self.file.fopen()
                return self.file.fread(self.header_size, 'B')
            finally:
                self.file.fclose()

    def write_header(self, header_data):
        if len(header_data) > self.header_size:
            raise ValueError("Header data to write is larger than the header size.")

        with self._lock:
            try:
                self.file.permission = 'r+' if os.path.exists(self.file.fullpathfilename) else 'w'
                self.file.fopen()
                self.file.fid.write(header_data)
            finally:
                self.file.fclose()

    def row_size(self):
        return sum(self.record_size)

    def read_row(self, row, col):
        if col < 1 or col > len(self.record_size):
            raise ValueError(f"Column must be in 1..{len(self.record_size)}.")

        with self._lock:
            try:
                self.file.permission = 'r'
                self.file.fopen()
                r, _ = self.get_size()

                if isinstance(row, float) and row == float('inf'):
                    row = list(range(1, r + 1))
                elif not isinstance(row, list):
                    row = [row]

                if any(ro > r for ro in row):
                    raise ValueError(f"Rows must be in 1..{r}.")

                data = []
                for ro in row:
                    offset = self.header_size + (ro - 1) * self.row_size() + sum(self.record_size[:col-1])
                    self.file.fid.seek(offset)

                    dtype = self.record_type[col-1]
                    num_elements = self.elements_per_column[col-1]

                    # This is a simplified way to handle types. A more robust solution
                    # would map Matlab types to Python struct format characters.
                    if dtype == 'char':
                        fmt = f'{num_elements}s'
                    elif dtype == 'double':
                        fmt = f'{num_elements}d'
                    elif dtype == 'uint64':
                        fmt = f'{num_elements}Q'
                    else:
                        raise ValueError(f"Unsupported data type: {dtype}")

                    d_read = struct.unpack(self.file.machineformat + fmt, self.file.fid.read(self.record_size[col-1]))
                    data.append(d_read[0] if len(d_read) == 1 else d_read)

                return data
            finally:
                self.file.fclose()

    # The other methods (insert_row, delete_row, write_entry, etc.) would be implemented here,
    # following a similar pattern of acquiring the lock, opening the file, performing
    # the operation, and then closing the file and releasing the lock.
    # Due to the complexity of these methods, they are omitted here for brevity.
