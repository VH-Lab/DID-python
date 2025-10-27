class FileObj:
    def __init__(self, fullpathfilename, machineformat='<', permission='r'):
        self.fullpathfilename = fullpathfilename
        self.machineformat = machineformat
        self.permission = permission
        self.fid = None

    def setproperties(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        return self

    def fopen(self):
        mode = self.permission
        if 'b' not in mode:
            mode += 'b'
        self.fid = open(self.fullpathfilename, mode)

    def fclose(self):
        if self.fid:
            self.fid.close()
            self.fid = None

    def fwrite(self, data, dtype):
        if self.fid:
            self.fid.write(data.tobytes())

    def fread(self, size, dtype):
        if self.fid:
            import numpy as np
            return np.fromfile(self.fid, dtype=dtype, count=size)
