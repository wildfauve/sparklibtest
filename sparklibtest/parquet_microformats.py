import pyarrow as pa

from . import parquet_base_types as B
from . import vocab_dictionary as V

# Price Types
type_id_amt_curr = pa.struct([B.at_type,
                              B.at_id,
                              pa.field('fibo-fnd-acc-cur:hasAmount', B.pa_dec_20_6),
                              pa.field('fibo-fnd-acc-cur:hasCurrency', B.pa_str)])

type_id_amt_curr_tag = pa.struct([B.at_type,
                                  B.at_id,
                                  pa.field(V.l_get('*.fibo-fnd-acc-cur:hasPrice.fibo-fnd-acc-cur:hasAmount'),
                                           B.pa_dec_20_6),
                                  pa.field(V.l_get('*.fibo-fnd-acc-cur:hasPrice.fibo-fnd-acc-cur:hasCurrency'),
                                           B.pa_str),
                                  pa.field(V.l_get('*.fibo-fnd-acc-cur:hasPrice.schema:priceCurrency'),
                                           B.pa_str)])

list_of_type_id_amt_curr = pa.list_(type_id_amt_curr, -1)
list_of_type_id_amt_curr_tag = pa.list_(type_id_amt_curr_tag, -1)

list_of_id = pa.list_(B.pa_str, -1)
