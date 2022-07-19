import pyarrow as pa

from . import vocab_dictionary as V
from . import parquet_base_types as B
from . import parquet_microformats as M

# Domain Types

pa_ts_custodian = pa.timestamp('s', tz='America/Chicago')
pa_ts_utc = pa.timestamp('s', tz='utc')

pa_observered_datatime = pa.field("fibo-fnd-dt-fd:hasObservedDateTime", pa_ts_utc)
pa_nav_datatime = pa.field("sfo-prt:hasNAVDateTime", pa_ts_utc)



pa_instrument_name = pa.struct(
    [pa.field(V.l_get("FinancialInstrument.fibo-sec-sec-iss:hasFinancialInstrumentShortName.lcc-lr:hasTag"),
              B.pa_str,
              metadata=V.term_meta("FinancialInstrument.fibo-sec-sec-iss:hasFinancialInstrumentShortName.lcc-lr:hasTag")),
     pa.field(V.l_get("FinancialInstrument.fibo-sec-sec-iss:hasFinancialInstrumentShortName.lcc-lr:isMemberOf"),
              B.pa_str,
              metadata=V.term_meta(
                  "FinancialInstrument.fibo-sec-sec-iss:hasFinancialInstrumentShortName.lcc-lr:isMemberOf"))])

sfo_position = pa.struct([B.at_type,
                          B.at_id,
                          pa_nav_datatime,
                          pa.field(V.l_get("Position_hasHeldPosition.fibo-sec-eq-eq:indicatesNumberOfShares"),
                                   B.pa_dec_20_6,
                                   metadata=V.term_meta(
                                       "Position_hasHeldPosition.fibo-sec-eq-eq:indicatesNumberOfShares")),
                          pa.field("fibo-fi-ip:hasPriceDeterminationMethod", B.pa_str),
                          pa.field("fibo-fnd-acc-cur:hasPrice", M.list_of_type_id_amt_curr)])


sfo_cost_valuation = pa.struct([B.at_type,
                          B.at_id,
                          pa_nav_datatime,
                          pa.field("fibo-sec-sec-ast:hasAcquisitionPrice", M.list_of_type_id_amt_curr)])


fi_short_name = pa.struct([B.at_type, pa.field("lcc-lr:hasTag", B.pa_str), pa.field("lcc-lr:isMemberOf", B.pa_str)])

instrument = pa.struct([B.at_type,
                        B.at_id,
                        pa.field(V.l_get("FinancialInstrument.fibo-sec-sec-iss:hasFinancialInstrumentShortName"),
                                 pa_instrument_name,
                                 metadata=V.term_meta(
                                     "FinancialInstrument.fibo-sec-sec-iss:hasFinancialInstrumentShortName")),
                        pa.field(V.l_get("FinancialInstrument.sfo:hasCountryOfRisk"),
                                 B.pa_str,
                                 metadata=V.term_meta("FinancialInstrument.sfo:hasCountryOfRisk")),
                        pa.field("fibo-fnd-arr-rt:hasRatingScore", B.list_of_type_id_tag),
                        pa.field("lcc-cr:isClassifiedBy", B.list_of_type_id_tag),
                        pa.field("lcc-cr:isIdentifiedBy", B.list_of_type_id_tag)])

instrument_market_price = pa.struct([B.at_type,
                                     pa_observered_datatime,
                                     pa.field(V.l_get("FinancialInstrument_hasMarketPrice.fibo-fnd-acc-cur:hasPrice"),
                                              M.list_of_type_id_amt_curr_tag,
                                              metadata=V.term_meta(
                                                  'FinancialInstrument_hasMarketPrice.fibo-fnd-acc-cur:hasPrice'))])

position_type = pa.struct([B.at_type,
                           pa_observered_datatime,
                           pa.field("indicatesNumberOfShares", B.pa_dec_20_6),
                           pa.field("fibo-fnd-acc-cur:hasPrice", M.list_of_type_id_amt_curr)])

issuer_type = pa.struct([B.at_id,
                         B.at_type,
                         pa.field("lcc-cr:isClassifiedBy", B.pa_str)])


def schema():
    return pa.schema([
        pa.field("Portfolio", B.type_id_label),
        pa.field("Position", B.type_id),
        pa.field("Position_hasHeldPosition", sfo_position),
        pa.field("Position_hasHeldPositionChange", sfo_position),
        pa.field("Position_hasCostValuation", sfo_cost_valuation),
        pa.field("FinancialInstrument", instrument),
        pa.field("FinancialInstrument_hasMarketPrice", instrument_market_price),
        pa.field("FinancialInstrument_hasMarketPriceChange", instrument_market_price),
        pa.field("Issuer", issuer_type)
    ])

def print_schema():
    print(schema().to_string())
    pass