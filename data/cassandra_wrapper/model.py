from collections import namedtuple

FIELDS_Quote= ('market_id','selection_id', 'status', 'timestamp',
               'total_matched', 'last_price_traded', 'inplay',
               'back_1', 'back_size_1','back_2', 'back_size_2','back_3', 'back_size_3',
               'lay_1', 'lay_size_1', 'lay_2', 'lay_size_2','lay_3', 'lay_size_3')

Quote = namedtuple('Quote', FIELDS_Quote)


FIELDS_Trades_min= ("date","event_name","timestamp","market_id","selection_id",
"inplay","ltp","status", "runner_name", "sort_priority",
"market_start_time","country_code","type","timezone",
"open_date","betting_type","market_type","event_id",
"status_market")

FIELDS_Trades_over_under= ("batb_price_0","batb_price_1","batb_price_2","batb_volume_0","batb_volume_1",
"batb_volume_2","batl_price_0","batl_price_1","batl_price_2",
"batl_volume_0","batl_volume_1","batl_volume_2","betting_type","country_code","date",
"event_id","event_name","inplay","ltp","market_id","market_start_time","market_type",
"open_date","runner_name","selection_id","sort_priority",
"status","status_market","timestamp","timezone","trd_vol","tv","type","vwap","timestamp_min")

FIELDS_Trades_over_under_basic = ("betting_type","country_code","date",
"event_id","event_name","inplay","ltp","market_id","market_start_time","market_type",
"open_date","runner_name","selection_id","sort_priority",
"status","status_market","timestamp","timezone","type")


Trades_min = namedtuple('Trades_min', FIELDS_Trades_min)

FIELDS_Trades= ('date', 'sports_id', 'event_id', 'settled_date', 'full_description', 'scheduled_off',
                'event', 'dt_actual_off', 'selection_id', 'selection', 'odds', 'number_bets',
                'volume_matched', 'latest_taken', 'first_taken', 'win_flag', 'in_play',
                'competition_type', 'competition', 'fixtures', 'event_name', 'market_type')

Trades = namedtuple('Trades', FIELDS_Trades)

