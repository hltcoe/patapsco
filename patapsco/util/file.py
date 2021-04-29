import pathlib

from ..error import ConfigError


def path_append(path, subdirectory):
    """Append a subdirectory to a path

    Args:
        path (str or Path): base path
        subdirectory (str): directory to add to path
    """
    return str(pathlib.Path(path) / subdirectory)


def validate_encoding(encoding):
    """Validate that this encoding is supported by python"""
    try:
        with open(__file__, 'r', encoding=encoding):
            pass
    except LookupError:
        raise ConfigError(f"{encoding} is not a valid file encoding")


def delete_dir(path):
    """Recursively delete a directory"""
    path = pathlib.Path(path)
    for child in path.glob('*'):
        if child.is_file():
            child.unlink()
        else:
            delete_dir(child)
    path.rmdir()


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
    count = 0
    with open(path, 'r', encoding=encoding) as fp:
        for _ in fp:
            count += 1
    return count


def count_lines_with(string, path, encoding='utf8'):
    """Count lines in a text file with a particular string"""
    count = 0
    with open(path, 'r', encoding=encoding) as fp:
        for line in fp:
            if string in line:
                count += 1
    return count
