from typing import Dict
from functools import lru_cache
import pyarrow as pa
from decimal import *

from . import vocab_dictionary as D


# Basic Types
# ===========
pa_int32 = pa.int32()
pa_int64 = pa.int64()
pa_str = pa.string()
pa_bin = pa.binary()
pa_bin_10 = pa.binary(10)
pa_dec_20_6 = pa.decimal128(20, 6)
pa_ts = pa.timestamp('ms')

# Struct types
# ============
at_type = pa.field("@type", pa_str)
at_id = pa.field("@id", pa_str)
at_label = pa.field("@label", pa_str)
has_tag = pa.field("lcc-lr:hasTag", pa_str)

# (@type, @id,)
type_id = pa.struct([at_type, at_id])

# (@type, @id, @label)
type_id_label = pa.struct([at_type, at_id, at_label])

# (@type, @id, lcc-lr:hasTag)
type_id_tag = pa.struct([at_type, at_id, has_tag])

list_of_type_id = pa.list_(type_id, -1)
list_of_type_id_tag = pa.list_(type_id_tag, -1)

# Price Types
type_id_amt_curr = pa.struct([at_type,
                              at_id,
                              pa.field('fibo-fnd-acc-cur:hasAmount', pa_dec_20_6),
                              pa.field('fibo-fnd-acc-cur:hasCurrency', pa_str)])

type_id_amt_curr_tag = pa.struct([at_type,
                                  at_id,
                                  pa.field(D.l_get('*.fibo-fnd-acc-cur:hasPrice.fibo-fnd-acc-cur:hasAmount'),
                                           pa_dec_20_6),
                                  pa.field(D.l_get('*.fibo-fnd-acc-cur:hasPrice.fibo-fnd-acc-cur:hasCurrency'),
                                           pa_str),
                                  pa.field(D.l_get('*.fibo-fnd-acc-cur:hasPrice.schema:priceCurrency'),
                                           pa_str)])

list_of_type_id_amt_curr = pa.list_(type_id_amt_curr, -1)
list_of_type_id_amt_curr_tag = pa.list_(type_id_amt_curr_tag, -1)

list_of_id = pa.list_(pa_str, -1)

pa.map_(pa_str, pa_str)