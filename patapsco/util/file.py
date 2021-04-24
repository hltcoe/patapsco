import glob
import pathlib

from ..error import BadDataError, ConfigError


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


class GlobFileGenerator:
    """
    You have a function that returns a generator given a file.
    You have one or more globs that match files.
    You want to seamlessly iterator over the function across the files that match.
    Use GlobFileGenerator.
    """

    def __init__(self, globs, func, *args, **kwargs):
        """
        Args:
            globs (list or str): array of glob strings or single glob string
            func (callable): parsing function returns a generator
            *args: variable length arguments for the parsing function
            **kwargs: keyword arguments for the parsing function
        """
        if isinstance(globs, str):
            globs = [globs]
        self.globs = iter(globs)
        self.parsing_func = func
        self.args = args
        self.kwargs = kwargs

        self._validate_globs(globs)

        self.pattern = None
        self.first_use_of_gen = True
        paths = self._next_glob()
        self.paths = iter(paths)
        self.gen = self._next_generator()

    def __iter__(self):
        return self

    def __next__(self):
        try:
            item = next(self.gen)
            self.first_use_of_gen = False
            return item
        except StopIteration:
            if self.first_use_of_gen:
                # bad file so we throw an exception
                raise BadDataError(f"{self.pattern} did not result in any items")
            try:
                self.gen = self._next_generator()
            except StopIteration:
                self.paths = iter(self._next_glob())
            return self.__next__()

    def _next_glob(self):
        self.pattern = next(self.globs)
        return sorted(glob.glob(self.pattern))

    def _next_generator(self):
        path = next(self.paths)
        self.first_use_of_gen = True
        return self.parsing_func(path, *self.args, **self.kwargs)

    @staticmethod
    def _validate_globs(globs):
        for pattern in globs:
            if not glob.glob(pattern):
                raise ConfigError(f"No files match pattern '{pattern}'")
