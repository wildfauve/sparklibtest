from typing import Dict
from enum import Enum
import json

from . import fn

class VocabType(Enum):
    ONTO = "ontology"
    PARQ = "parquet"

"""
The Vocab access is based on the json path from the interim JSON file created from the NT data (e.g. see cbor/data/deltas).

Taking a snippet...
"Position_hasHeldPosition": {
    "fibo-sec-eq-eq:indicatesNumberOfShares": "140313.00000",
    "fibo-fnd-acc-cur:hasPrice": [
        {
            "fibo-fnd-acc-cur:hasAmount": "0000002423205.51000",
            "fibo-fnd-acc-cur:hasCurrency": "fibo-fnd-acc-4217:AUD",
            "schema:priceCurrency": "AUD"
        }
    ]
}

The path to fibo-sec-eq-eq:indicatesNumberOfShares would be Position_hasHeldPosition.fibo-sec-eq-eq:indicatesNumberOfShares

At that location in the Vocab we have;

"Position_hasHeldPosition": {
    "fibo-sec-eq-eq:indicatesNumberOfShares": {
        "termType": "objectProperty",
        "label": "The held position as at an observed datetime.  Either a change in that position or the absolute position.",
        'hasDataProductTerm': 'numberOfShares',
    }
}

From here, the term fibo-sec-eq-eq:indicatesNumberOfShares is defined as the Parquet field numberOfShares using the vocab property 'hasDataProductTerm'.

Further more the Parquet field metadata is generated from, 'termType', 'label' and the path.  




"""

vocab = {
    "": {},
    "FinancialInstrument": {
        "fibo-sec-sec-iss:hasFinancialInstrumentShortName": {
            "termType": "objectProperty",
            "label": "The financial instrument short name structure, containing tag and scheme membership.",
            'hasDataProductTerm': 'financialInstrumentShortName',
            "lcc-lr:hasTag": {
                "termType": "dataProperty",
                "hasDataProductTerm": "financialInstrumentShortName",
                "label": "The short name provided by the Custodian."},
            "lcc-lr:isMemberOf": {
                "termType": "dataProperty",
                "hasDataProductTerm": "memberOf",
                'label': "URI from the ontology identifying the scheme vocab."}
        },
        "sfo:hasCountryOfRisk": {
            "termType": "dataProperty",
            "hasDataProductTerm": "countryOfRisk",
            'label': "URI from the ontology identifying the scheme vocab."
        }
    },
    "FinancialInstrument_hasMarketPrice": {
      "fibo-fnd-acc-cur:hasPrice": {
          "termType": "objectProperty",
          "label": "The price used by the custodian to value the current position.  A collection of prices in multiple currencies, usually the base and local currency.",
          'hasDataProductTerm': 'marketPrice',
      }
    },
    "Position_hasHeldPosition": {
        "fibo-sec-eq-eq:indicatesNumberOfShares": {
            "termType": "objectProperty",
            "label": "The held position as at an observed datetime.  Either a change in that position or the absolute position.",
            'hasDataProductTerm': 'numberOfShares',
        }
    },
    "*": {
        "fibo-fnd-acc-cur:hasPrice": {
            "termType": "objectProperty",
            "label": "An individual price, in the common FIBO currency amount format.",
            "fibo-fnd-acc-cur:hasAmount": {
                'hasDataProductTerm': "amount"
            },
            'fibo-fnd-acc-cur:hasCurrency': {
                'hasDataProductTerm': "currencyUri"
            },
            'schema:priceCurrency': {
                'hasDataProductTerm': "currency"
            }
        }
    }
}

## API

def m_get(path):
    return meta_serialise(m_for(path))


def l_get(path):
    return t_for(path, VocabType.PARQ)


def term_meta(path):
    return {"term": m_get(path)}


def meta_serialise(meta: Dict):
    return json.dumps(meta)



##


def t_for(path: str, vocab_type: VocabType = VocabType.ONTO) -> str:
    path_array, term = term_finder(path)
    if not term:
        breakpoint()
    if vocab_type == VocabType.ONTO:
        return path_array[-1]
    return term.get('hasDataProductTerm', None)

def m_for(path: str) -> Dict:
    path_array, term = term_finder(path)
    return {
        "prefixedTermPath": path,
        "termType": term['termType'],
        "label": term['label'],
    }

def term_finder(path):
    path_array = path.split(".")
    term = fn.deep_get(vocab, path_array)
    return path_array, term
