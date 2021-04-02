import io

import pytest
import yaml

import pipeline.config as config


def test_convert_dict_with_nested_list():
    d = {
        'key1': 64,
        'key2': 'test',
        'key3': [{'a': 1}, {'b': 2}]
    }
    d = config.convert_dict(d)
    assert len(d.items()) == 3
    assert type(d.key3[0]) is config.AttrDict
    assert d.key3[0].a == 1


def test_convert_dict_with_nested_dict():
    d = {
        'key1': 64,
        'key2': 'test',
        'key3': {'a': 1, 'b': {'value': 42}}
    }
    d = config.convert_dict(d)
    assert len(d.items()) == 3
    assert type(d.key3) is config.AttrDict
    assert d.key3.b.value == 42


def test_simple_config():
    document = """
lang: es
output: myfile.txt
document_process:
  utf8_normalize: true
  lowercase: true
score:
  - map
  - p@20
  - ndcg
"""
    conf = config.load_yaml_config(document)
    assert conf['lang'] == 'es'
    assert type(conf['document_process']) is dict
    assert type(conf['score']) is list
    assert conf['document_process']['lowercase'] is True
    assert conf['score'][0] == "map"


def test_interpolation_simple():
    document = """
output: output_{lang}
lang: es
"""
    conf = yaml.load(document, Loader=config.ConfigLoader)
    assert conf['lang'] == 'es'
    assert conf['output'] == 'output_es'


def test_interpolation_at_string_start():
    # cannot start with {value}, must quote it
    document = """
output: {lang}_output
lang: es
"""
    with pytest.raises(yaml.MarkedYAMLError):
        config.load_yaml_config(document)


def test_interpolation_at_start_with_quote():
    # cannot start with {value}, must quote it
    document = """
output: "{lang}_output"
lang: es
"""
    conf = config.load_yaml_config(document)
    assert conf['output'] == 'es_output'


def test_interpolation_with_nest_value():
    document = """
lang: ru
document_process:
  utf8_normalize: true
  lowercase: true
  stem:
    name: pymorphy2
    param1: 0.5
  output: ru-{document_process.stem.name}-{document_process.stem.param1}
"""
    conf = config.load_yaml_config(document)
    assert conf['document_process']['output'] == "ru-pymorphy2-0.5"


def test_interpolation_with_missing_value():
    document = """
lang: ru
document_process:
  utf8_normalize: true
  lowercase: true
  stem:
    name: pymorphy2
  output: ru-{document_process.stem.name}-{document_process.stem.param1}
"""
    with pytest.raises(config.ConfigError):
        config.load_yaml_config(document)


def test_interpolation_cascade():
    document = """
a: 1
b: "{a}1"
c: "{b}1"
"""
    conf = yaml.load(document, Loader=config.ConfigLoader)
    assert conf['b'] == '11'
    assert conf['c'] == '111'


def test_interpolation_cascade_with_wrong_order():
    document = """
a: 1
c: "{b}1"
b: "{a}1"
"""
    conf = yaml.load(document, Loader=config.ConfigLoader)
    assert conf['b'] == '11'
    assert conf['c'] == '{a}11'


def test_load_json_config():
    document = """
{
  "lang": "es",
  "output": "myfile.txt",
  "document_process": {
    "utf8_normalize": true,
    "lowercase": true
  },
  "score": ["map", "p@20", "ndcg"]
}
"""
    conf = config.load_json_config(io.StringIO(document))
    assert conf['lang'] == 'es'
    assert type(conf['document_process']) is dict
    assert type(conf['score']) is list
    assert conf['document_process']['lowercase'] is True
    assert conf['score'][0] == "map"


def test_json_interpolation_simple():
    document = """
{
  "output": "output_{lang}",
  "lang": "es"
}
"""
    conf = config.load_json_config(io.StringIO(document))
    assert conf['lang'] == 'es'
    assert conf['output'] == 'output_es'


def test_json_interpolation_with_missing_value():
    document = """
{
  "lang": "ru",
  "document_process": {
    "utf8_normalize": true,
    "lowercase": true,
    "stem": {
      "name": "pymorphy2"
    }
  },
  "output": "ru-{document_process.stem.name}-{document_process.stem.param1}"
}
"""
    with pytest.raises(config.ConfigError):
        config.load_json_config(io.StringIO(document))


def test_json_interpolation_cascade():
    document = """
{
"a": 1,
"b": "{a}1",
"c": "{b}1"
}
"""
    conf = yaml.load(document, Loader=config.ConfigLoader)
    assert conf['b'] == '11'
    assert conf['c'] == '111'


def test_json_interpolation_cascade_with_wrong_order():
    document = """
{
"a": 1,
"c": "{b}1",
"b": "{a}1"
}
"""
    conf = yaml.load(document, Loader=config.ConfigLoader)
    assert conf['b'] == '11'
    assert conf['c'] == '{a}11'


def test_overrides():
    conf = {
        'a': 'one',
        'b': 'two',
        'c': 'three'
    }
    config.ConfigOverrides.process(conf, ["b=new"])
    assert conf['a'] == 'one'
    assert conf['b'] == 'new'


def test_override_missing():
    conf = {
        'a': 'one',
        'b': 'two',
        'c': 'three'
    }
    with pytest.raises(config.ConfigError):
        config.ConfigOverrides.process(conf, ["d=error"])


def test_nested_overrides():
    conf = {
        'a': 'one',
        'b': 'two',
        'c': {
            'x': 'value'
        }
    }
    config.ConfigOverrides.process(conf, ["c.x=new"])
    assert conf['c']['x'] == 'new'


def test_nested_overrides_missing():
    conf = {
        'a': 'one',
        'b': 'two',
        'c': {
            'x': 'value'
        }
    }
    with pytest.raises(config.ConfigError):
        config.ConfigOverrides.process(conf, ["d.x=new"])

    with pytest.raises(config.ConfigError):
        config.ConfigOverrides.process(conf, ["c.z=new"])
