def rmrf(directory):
    for item in directory.iterdir():
        if item.is_dir():
            rmrf(item)
        elif item.is_file():
            item.unlink()
    directory.rmdir()
