import pathlib


def delete_dir(path):
    """Recursively delete a directory"""
    path = pathlib.Path(path)
    for child in path.glob('*'):
        if child.is_file():
            child.unlink()
        else:
            delete_dir(child)
    path.rmdir()
