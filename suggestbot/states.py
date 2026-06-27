from aiogram.fsm.state import State, StatesGroup


class UserSuggest(StatesGroup):
    waiting_content = State()   # ждём контент от юзера
    confirming = State()        # юзер подтверждает отправку


class AdminStates(StatesGroup):
    replying = State()          # админ пишет ответ юзеру
    editing = State()           # админ редактирует контент перед постом
    add_channel = State()       # добавление канала (одноразовый флоу)
