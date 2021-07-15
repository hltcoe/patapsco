import gzip
import pathlib
import shutil

from ..error import ConfigError


def path_append(path, subdirectory):
    """Append a subdirectory to a path

    Args:
        path (str or Path): base path
        subdirectory (str): directory to add to path

    Returns:
        str
    """
    return str(pathlib.Path(path) / subdirectory)


def create_path(path):
    """Create a Path object expanding ~ for the user's home directory"""
    path = pathlib.Path(path)
    return path.expanduser()


def validate_encoding(encoding):
    """Validate that this encoding is supported by python"""
    try:
        with open(__file__, 'r', encoding=encoding):
            pass
    except LookupError:
        raise ConfigError(f"{encoding} is not a valid file encoding")


def delete_dir(path):
    """Recursively delete a directory"""
    shutil.rmtree(path)


def is_dir_empty(path):
    """Test whether the directory is empty of files"""
    return not any(pathlib.Path(path).iterdir())


def touch_complete(path):
    """Touch a file called .complete in the directory"""
    file = pathlib.Path(path) / ".complete"
    file.touch()


def is_complete(path):
    """Check if the .complete file exists in directory"""
    if not pathlib.Path(path).exists:
        return False
    file = pathlib.Path(path) / ".complete"
    return file.exists()


def count_lines(path, encoding='utf8'):
    """Count lines in a text file"""
    if isinstance(path, pathlib.Path):
        path = str(path)
    if path.endswith('.gz'):
        return gzip_count_lines(path)
    count = 0
    with open(path, 'r', encoding=encoding) as fp:
        for _ in fp:
            count += 1
    return count


def gzip_count_lines(path):
    count = 0
    with gzip.open(path, 'rb') as fp:
        for _ in fp:
            count += 1
    return count


def count_lines_with(string, path, encoding='utf8'):
    """Count lines in a text file with a particular string"""
    if path.endswith('.gz'):
        return gzip_count_lines_with(string, path, encoding)
    count = 0
    with open(path, 'r', encoding=encoding) as fp:
        for line in fp:
            if string in line:
                count += 1
    return count


def gzip_count_lines_with(string, path, encoding):
    bstr = string.encode(encoding)
    count = 0
    with gzip.open(path, 'rb') as fp:
        for line in fp:
            if bstr in line:
                count += 1
    return count
