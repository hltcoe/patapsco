import io

import pytest
import yaml

import pipeline.config as config


def test_flat_dict_get():
    d = {
        'a': 12,
        'b': {
            'c': 42
        }
    }
    fd = config.FlatDict(d)
    assert fd['a'] == 12
    assert fd['b.c'] == 42
    with pytest.raises(KeyError):
        test = fd['c']
    with pytest.raises(KeyError):
        test = fd['b.z']


def test_flat_dict_set():
    d = {
        'a': 12,
        'b': {
            'c': 42
        }
    }
    fd = config.FlatDict(d)
    fd['a'] = 99
    fd['b.c'] = 0
    assert d['a'] == 99
    assert d['b']['c'] == 0
    with pytest.raises(KeyError):
        fd['c'] = 77
    with pytest.raises(KeyError):
        fd['b.z'] = 77


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
    conf = config.ConfigService._read_yaml_config(document)
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
    conf = config.ConfigService._read_yaml_config(document)
    assert conf['lang'] == 'es'
    assert conf['output'] == 'output_es'


def test_interpolation_at_string_start():
    # cannot start with {value}, must quote it
    document = """
output: {lang}_output
lang: es
"""
    with pytest.raises(yaml.MarkedYAMLError):
        config.ConfigService._read_yaml_config(document)


def test_interpolation_at_start_with_quote():
    # cannot start with {value}, must quote it
    document = """
output: "{lang}_output"
lang: es
"""
    conf = config.ConfigService._read_yaml_config(document)
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
    conf = config.ConfigService._read_yaml_config(document)
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
        config.ConfigService._read_yaml_config(document)


def test_interpolation_cascade():
    document = """
a: 1
b: "{a}1"
c: "{b}1"
"""
    conf = config.ConfigService._read_yaml_config(document)
    assert conf['b'] == '11'
    assert conf['c'] == '111'


def test_interpolation_cascade_with_wrong_order():
    document = """
a: 1
c: "{b}1"
b: "{a}1"
"""
    conf = config.ConfigService._read_yaml_config(document)
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
    conf = config.ConfigService()._read_json_config(io.StringIO(document))
    assert conf['lang'] == 'es'
    assert type(conf['document_process']) is dict
    assert type(conf['score']) is list
    assert conf['document_process']['lowercase'] is True
    assert conf['score'][0] == "map"


def test_load_json_config_booleans():
    document = """
{
  "lowercase": "on",
  "stem": "off",
  "scores": [
    {"value": "yes"}
  ]
}
"""
    conf = config.ConfigService()._read_json_config(io.StringIO(document))
    assert conf['lowercase'] is True
    assert conf['stem'] is False
    assert conf['scores'][0]['value'] is True


def test_json_interpolation_simple():
    document = """
{
  "output": "output_{lang}",
  "lang": "es"
}
"""
    conf = config.ConfigService()._read_json_config(io.StringIO(document))
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
        config.ConfigService()._read_json_config(io.StringIO(document))


def test_json_interpolation_cascade():
    document = """
{
"a": 1,
"b": "{a}1",
"c": "{b}1"
}
"""
    conf = config.ConfigService()._read_json_config(io.StringIO(document))
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
    conf = config.ConfigService()._read_json_config(io.StringIO(document))
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


def test_inheritance():
    conf = {
        'a': {
            'p1': 1,
            'p2': 2,
        },
        'b': {
            'inherit': 'a',
            'p2': 0,
            'p3': 3
        }
    }
    config.ConfigInheritance.process(conf)
    assert conf['b']['p1'] == 1
    assert conf['b']['p2'] == 0
    assert conf['b']['p3'] == 3
    with pytest.raises(KeyError):
        test = conf['b']['inherit']


def test_inheritance_nested():
    conf = {
        'a': {
            'p1': 1,
            'p2': 2,
            'subsection': {
                'param': 'test'
            }
        },
        'b': {
            'inherit': 'a',
            'p2': 0,
            'p3': 3,
            'subsection': {
                'output': True
            }
        }
    }
    config.ConfigInheritance.process(conf)
    assert conf['b']['subsection']['param'] == 'test'
    assert conf['b']['subsection']['output'] is True


def test_inheritance_missing():
    conf = {
        'a': {
            'p1': 1,
            'p2': 2,
        },
        'b': {
            'inherit': 'z',
            'p2': 0,
            'p3': 3
        }
    }
    with pytest.raises(config.ConfigError):
        config.ConfigInheritance.process(conf)


def test_inheritance_nested_key():
    conf = {
        'a': {
            'b': {
                'color': 'red'
            },
            'p2': 2,
        },
        'x': {
            'y': {
                'inherit': 'a.b',
                'size': 'large'
            }
        }
    }
    config.ConfigInheritance.process(conf)
    assert conf['x']['y']['color'] == 'red'
    assert conf['x']['y']['size'] == 'large'
