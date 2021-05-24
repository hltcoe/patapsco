import argparse
import pathlib

from patapsco.util.normalize import NormalizerFactory


def main():
    parser = argparse.ArgumentParser(description="Normalize a text file. Output to stdout.")
    parser.add_argument("-l", "--lang", help="ISO 639-3 language code")
    parser.add_argument("file", help="Path to text file to normalize")
    args = parser.parse_args()

    normalizer = NormalizerFactory.create(args.lang)
    text = pathlib.Path(args.file).read_text()
    print(normalizer.normalize(text))


if __name__ == '__main__':
    main()
