from aiogram.fsm.state import State, StatesGroup


class CreateDealFSM(StatesGroup):
    waiting_amount = State()


class AggregatorSetupFSM(StatesGroup):
    choosing_aggregator = State()
    waiting_api_key = State()
    waiting_shop_id = State()   # for payok / freekassa
    waiting_secret = State()    # for freekassa
